"""
run.py (general.nih.curate)
===========================

Curate NiH data, ready for ingestion to the general ES endpoint.
"""

from nesta.core.luigihacks.elasticsearchplus import _null_empty_str
from nesta.core.luigihacks.elasticsearchplus import _clean_up_lists
from nesta.core.luigihacks.elasticsearchplus import _remove_padding
from nesta.core.luigihacks.elasticsearchplus import _country_detection
from nesta.packages.geo_utils.lookup import get_us_states_lookup
from nesta.packages.geo_utils.lookup import get_continent_lookup
from nesta.packages.geo_utils.lookup import get_country_continent_lookup
from nesta.packages.geo_utils.lookup import get_eu_countries
from nesta.packages.geo_utils.country_iso_code import country_iso_code
from nesta.packages.geo_utils.geocode import _geocode
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.orm_utils import object_to_dict

# ORMs
from nesta.core.orms.nih_orm import Projects, Abstracts
from nesta.core.orms.nih_orm import TextDuplicate
from nesta.core.orms.general_orm import NihProject, Base

from ast import literal_eval
import boto3
from collections import defaultdict
from datetime import datetime
import dateutil.parser
from itertools import groupby, chain
import json
import logging
from operator import attrgetter
import os
from sqlalchemy.orm import load_only


# Alias these fields, as they're verbose and used a lot
PK_ID = Projects.application_id
CORE_ID = Projects.base_core_project_num
DATETIME_COLS = [c for c in Projects.__table__.columns
                 if c.type.python_type is datetime]

# Group different types of field together, as they will be treated
# in a common way on aggregation
DATETIME_FIELDS = [c.name for c in DATETIME_COLS]
FLAT_FIELDS = ["application_id", "base_core_project_num", "fy",
               "org_city", "org_country", "org_name", "org_state",
               "org_zipcode", "project_title", "ic_name", "phr",
               "abstract_text"]
LIST_FIELDS = ["clinicaltrial_ids", "clinicaltrial_titles", "patent_ids",
               "patent_titles", "pmids", "project_terms"]


# Geo names edge cases
CTRY_LOOKUP = {'Korea Rep Of': 'Korea, Republic of',
               'Russia': 'Russian Federation',
               'Congo Dem Rep': 'Congo, The Democratic Republic of the',
               "Cote D'ivoire": "Côte d'Ivoire",
               'Dominican Rep': 'Dominican Republic',
               'Eswatini': 'Swaziland',
               'Fed Micronesia': 'Micronesia, Federated States of',
               'Papua N Guinea': 'Papua New Guinea',
               'St Kitts/nevis': 'Saint Kitts and Nevis',
               'St Lucia': 'Saint Lucia',
               'Tanzania U Rep': 'Tanzania',
               'Trinidad/toba': 'Trinidad and Tobago'}


def get_projects_by_appl_id(engine, appl_ids, nrows=None,
                            pull_relationships=None):
    """Get NiH projects by application ID (i.e. the primary key)
    and assign each project to it's own group (i.e. a group size of 1).
    This method is meant for projects with a NULL core ID, which therefore
    can't be grouped into a family of projects. Note that the argument
    `pull_relationships` is a dummy argument so that this function
    can be used as a template alongside `group_projects_by_core_id`.
    """
    # Get all projects in the given set of IDs
    filter_stmt = PK_ID.in_(appl_ids)
    with db_session(engine) as sess:
        q = sess.query(Projects).filter(filter_stmt).order_by(PK_ID)
        results = q.limit(nrows).all()
        # "Fake" single project groups
        groups = [[object_to_dict(obj, shallow=False,
                                  properties=True)]
                  for obj in results]
    return groups


def group_projects_by_core_id(engine, core_ids, nrows=None,
                              pull_relationships=False):
    """Get NiH projects by the base core project number ("core id"),
    and then group projects by this core id. If `pull_relationships`
    is True, then also unbundle any SqlAlchemy "relationship" objects;
    although this isn't required (and therefore substantially speeds
    things up) when, for example, only IDs are required."""
    # Get all projects in the given set of IDs
    filter_stmt = CORE_ID.in_(core_ids)
    with db_session(engine) as sess:
        q = sess.query(Projects).filter(filter_stmt).order_by(CORE_ID)
        results = q.limit(nrows).all()
        # Group the results by the core project number
        groups = [[object_to_dict(obj, shallow=not pull_relationships,
                                  properties=True)
                   for obj in group]
                  for _, group in
                  groupby(results, attrgetter('base_core_project_num'))]
    return groups


def get_sim_weights(dupes, appl_ids):
    """Retrieve the similarity weights for this project"""
    sim_weights = defaultdict(list)
    for d in dupes:
        appl_id_1 = d['application_id_1']
        appl_id_2 = d['application_id_2']
        # Referring to Note a) in `retrieve_similar_projects`,
        # determine which ID is the PK for the similar project
        id_ = appl_id_1 if appl_id_1 not in appl_ids else appl_id_2
        sim_weights[id_].append(d['weight'])
    # Match against the largest weight, if the similar project
    # has been retrieved multiple times
    sim_weights = {id_: max(weights)
                   for id_, weights in sim_weights.items()}
    return sim_weights


def retrieve_similar_projects(engine, appl_ids):
    """Retrieve all projects which are similar to those in this
    project group. Some of the similar projects will be retrieved
    multiple times if matched to multiple projects in the group.
    `appl_ids` is the set of IDs in this group.
    """
    # Note a) the TextDuplicate table doesn't double-count
    # application IDs, so the application IDs of this group
    # could be in either application_id_1 or application_id_2
    either = (TextDuplicate.application_id_1.in_(appl_ids) |
              TextDuplicate.application_id_2.in_(appl_ids))
    both = (TextDuplicate.application_id_1.in_(appl_ids) &
            TextDuplicate.application_id_2.in_(appl_ids))
    # We want either application_id_1 or application_id_2, but
    # not both, since in such a case both projects would already be
    # in the same group.
    filter_stmt = (either & ~both)
    with db_session(engine) as session:
        dupes = session.query(TextDuplicate).filter(filter_stmt).all()
        dupes = [object_to_dict(obj, shallow=True) for obj in dupes]

    # Get the similarity weights for this project
    sim_weights = get_sim_weights(dupes, appl_ids)
    sim_ids = set(sim_weights.keys())

    # Retrieve only the required fields by project id
    filter_stmt = PK_ID.in_(sim_ids)
    query_fields = [PK_ID, CORE_ID, Projects.fy, *DATETIME_COLS]
    with db_session(engine) as session:
        q = session.query(*query_fields).filter(filter_stmt)
        sim_projs = [{field.name: value
                      for field, value in zip(query_fields, values)}
                     for values in q.all()]
    return sim_projs, sim_weights


def earliest_date(project):
    """Determine the earliest date, among all the date fields
    in this project. Returns `datetime.min` if no date is found."""
    year = project['fy']
    # Try to find a date
    dates = []
    for f in DATETIME_FIELDS:
        date = project[f]
        if type(date) is str:
            date = dateutil.parser.parse(date)
        elif date is None:
            continue
        dates.append(date)
    min_date = datetime.min  # default value if no date fields present
    if len(dates) > 0:
        min_date = min(dates)
    # Otherwise, fall back on the year field
    elif year is not None:
        min_date = datetime(year=year, month=1, day=1)
    return min_date


def retrieve_similar_proj_ids(engine, appl_ids):
    """Retrieve similar projects, expand each similar
    project into its group using the core ID.
    Then extract only the most recent PK ID from each group,
    and group these PK IDs by their similarity score, in order
    to have lists of "near duplicates", "very similar" and
    "fairly similar" IDs. `appl_ids` is the set of IDs in this group,
    from which similar projects are to be found.
    """
    # Retrieve similar projects
    projs, weights = retrieve_similar_projects(engine, appl_ids)
    # Retrieve core IDs in order to perform groupby on the
    # similar projects
    groups = []
    core_ids = set()
    for proj in projs:
        core_id = proj["base_core_project_num"]
        # Around 3% of projs have no core id, and hence no group
        if core_id is None:
            groups.append([proj])
        else:
            core_ids.add(core_id)
    groups += group_projects_by_core_id(engine, core_ids)

    # Return just the PK of the most recent project in each group
    pk_weights = {}
    for group in groups:
        # Get the most recent project
        sorted_group = sorted(group, key=earliest_date, reverse=True)
        pk0 = sorted_group[0]['application_id']
        # Get the maximum similarity of any project in the group
        pks = set(proj['application_id'] for proj in group)
        max_weight = max(weights[pk] for pk in pks if pk in weights)
        pk_weights[pk0] = max_weight

    # Group projects by their similarity
    similar_projs = group_projs_by_similarity(pk_weights)
    return similar_projs


def group_projs_by_similarity(pk_weights,
                              ranges = {'near_duplicate_ids': (0.8, 1),
                                        'very_similar_ids': (0.65, 0.8),
                                        'fairly_similar_ids': (0.4, 0.65)}):
    """Group projects by range of similarity. Ranges have been
    hand-selected, and clearly are subject to optimisation."""
    grouped_projs = {label: [pk for pk, weight in pk_weights.items()
                             if weight > lower and weight <= upper]
                     for label, (lower, upper) in ranges.items()}
    return grouped_projs


def combine(func, list_of_dict, key):
    """Apply the given function over the values retrieved
    by the given key for each item in a of dictionaries"""
    values = [_dict[key] for _dict in list_of_dict
              if _dict[key] is not None]
    if len(values) == 0:
        return None
    return func(values)


def first_non_null(values):
    """Return the first non-null value in the list"""
    for v in values:
        if v is None:
            continue
        return v
    return None


def join_and_dedupe(values):
    """Flatten the list and deduplicate"""
    return list(set(chain(*values)))


def format_us_zipcode(zipcode):
    """NiH US postcodes have wildly inconsistent formatting,
    leading to geocoding errors. If the postcode if greater
    than 5 chars, it should be in the format XXXXX-XXXX,
    or XXXXX, even if the first 5 chars require zero-padding."""
    ndigits = len(zipcode)
    # Only apply the procedure to numeric postcodes like
    if not zipcode.isnumeric():
        return zipcode
    # e.g 123456789 --> 12345-6789
    # or    3456789 --> 00345-6789
    if ndigits > 5:
        start, end = zipcode[:-4].zfill(5), zipcode[-4:]
        return f'{start}-{end}'
    # e.g 12345 --> 12345
    # or    345 --> 00345
    else:
        return zipcode.zfill(5)


def geocode(city, state, country, postalcode):
    """Apply the OSM geocoding for as many fields as possible."""
    kwargs = {'city': city,
              'state': state,
              'country': country,
              'postalcode': postalcode}
    # Ditch null kwargs
    kwargs = {k: v for k, v in kwargs.items()
              if v is not None}
    if len(kwargs) == 0:
        return None
    # Try with the postal code (doesn't always work, but when
    # it does it gives more accurate results)
    coords = _geocode(**kwargs)
    # Otherwise, try removing the postcode
    if coords is None and 'postalcode' in kwargs:
        del kwargs['postalcode']
        coords = _geocode(**kwargs)
    # If still no results, try a plain query (tends to give
    # very coarse resolution)
    if coords is None:
        coords = _geocode(q=', '.join(kwargs.values()))
    return coords


def aggregate_group(group):
    """Aggregate fields from all projects in this group into a
    single curated pseudo-project."""
    # Sort by most recent first
    group = list(sorted(group, key=earliest_date, reverse=True))
    project = {"grouped_ids": [p['application_id'] for p in group],
               "grouped_titles": [p['project_title'] for p in group]}

    # Extract the first non-null fields directly from PROJECT_FIELDS
    for field in FLAT_FIELDS:
        project[field] = combine(first_non_null, group, field)
    # Concat list fields
    for field in LIST_FIELDS:
        project[field] = combine(join_and_dedupe, group, field)
    # Specific aggregrations
    project["project_start"] = combine(min, group, "project_start")
    project["project_end"] = combine(max, group, "project_end")
    project["total_cost"] = combine(sum, group, "total_cost")

    # Extra specific aggregrations for yearly funds
    yearly_groups = defaultdict(list)
    for proj in group:
        date = earliest_date(proj)
        if date == datetime.min:  # i.e. no date found
            continue
        yearly_groups[date.year].append(proj)
    # Combine by year group
    yearly_funds = [{"year": year,
                     "project_start": combine(min, yr_group, "project_start"),
                     "project_end": combine(max, yr_group, "project_end"),
                     "total_cost": combine(sum, yr_group, "total_cost")}
                    for year, yr_group in yearly_groups.items()]
    project["yearly_funds"] = sorted(yearly_funds, key=lambda x: x['year'])
    return project


def extract_geographies(row):
    """Infer standard geographic info for this row"""

    # Lookup helpers (note, all are lru_cached)
    states_lookup = get_us_states_lookup()
    ctry_continent_lookup = get_country_continent_lookup()
    continent_lookup = get_continent_lookup()
    eu_countries = get_eu_countries()

    # If country name is badly formatted, reassign
    ctry = row['org_country']
    if ctry in CTRY_LOOKUP:
        ctry = CTRY_LOOKUP[ctry]

    # Perform lookups
    iso2 = None
    if ctry is not None:
        iso_info = country_iso_code(ctry)
        iso2 = iso_info.alpha_2
        row['org_country'] = iso_info.name  # Standardise country naming
    row['iso2'] = iso2
    row['is_eu'] = iso2 in eu_countries
    row['state_name'] = states_lookup[row['org_state']]
    continent_iso2 = ctry_continent_lookup[iso2]
    row['continent_iso2'] = continent_iso2
    row['continent_name'] = continent_lookup[continent_iso2]

    # Clean zip code if US
    if iso2 == 'US' and row['org_zipcode'] is not None:
        row['org_zipcode'] = format_us_zipcode(row['org_zipcode'])

    # Retrieve lat / lon for this org
    row['coordinates'] = geocode(city=row['org_city'],
                                 state=row['state_name'],
                                 country=row['org_country'],
                                 postalcode=row['org_zipcode'])
    return row


def apply_cleaning(row):
    """Curate raw data for ingestion to MySQL."""
    row = _country_detection(row, 'country_mentions')
    row = _remove_padding(row)
    row = _null_empty_str(row)
    row = _clean_up_lists(row)
    return row


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    using_core_ids = literal_eval(os.environ["BATCHPAR_using_core_ids"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    os.environ["MYSQLDB"] = os.environ["BATCHPAR_config"]

    # Database setup
    engine = get_mysql_engine("MYSQLDB", "mysqldb", db_name)

    # Retrieve list of core ids from s3
    nrows = 1000 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    core_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(core_ids)} ids retrieved from s3")

    # Get the groups for this batch.
    # Around 3% of core ids are null, and so these are retrieved
    # in batches of application id instead, and otherwise pull
    # in the projects with the non-null core id as these can
    # be aggregated together.
    data_getter = (group_projects_by_core_id if using_core_ids
                   else get_projects_by_appl_id)
    groups = data_getter(engine, core_ids, pull_relationships=True)

    # Curate each group
    data = []
    for group in groups:
        appl_ids = [proj['application_id'] for proj in group]
        similar_projs = retrieve_similar_proj_ids(engine, appl_ids)
        project = aggregate_group(group)
        geographies = extract_geographies(project)
        row = {**project, **geographies, **similar_projs}
        row = apply_cleaning(row)
        data.append(row)

    # Insert data into the database
    insert_data("MYSQLDB", "mysqldb", db_name, Base,
                NihProject, data, low_memory=True)
    logging.info("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
