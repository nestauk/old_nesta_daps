import logging
import re

from nesta.packages.misc_utils.batches import split_batches
from nesta.packages.misc_utils.sparql_query import sparql_query
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.mag_orm import FieldOfStudy


MAG_ENDPOINT = 'http://ma-graph.org/sparql'


def _batch_query_entities(ids=None, batch_size=50):
    query_template = '''PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX magc: <http://ma-graph.org/class/>
    PREFIX magp: <http://ma-graph.org/property/>

    SELECT ?field ?name ?level ?category
           GROUP_CONCAT(DISTINCT ?parent; separator=",") as ?parents
           GROUP_CONCAT(?child; separator=",") as ?children
    WHERE {{
        ?field rdf:type magc:FieldOfStudy .
        ?field magp:level ?level .
        OPTIONAL {{ ?field foaf:name ?name }}
        OPTIONAL {{ ?field magp:hasParent ?parent }}
        OPTIONAL {{ ?child magp:hasParent ?field }}
        {entity_filter}
    }}
    GROUP BY ?field ?name ?level ?category'''

    if not 1 <= batch_size <= 50:  # max limit for uri length
        raise ValueError("batch_size must be between 1 and 50")

    if ids is None:
        # retrieve all fields of study
        for batch in sparql_query(MAG_ENDPOINT, query_template.format(entity_filter='')):
            for row in batch:
                yield row
    else:
        for batch_of_ids in split_batches(ids, batch_size):
            entities = ','.join(f"<http://ma-graph.org/entity/{i}>" for i in batch_of_ids)
            entity_filter = f"FILTER (?field IN ({entities}))"
            for batch in sparql_query(MAG_ENDPOINT,
                                      query_template.format(entity_filter=entity_filter)):
                for row in batch:
                    yield row


def extract_entity_id(entity):
    """Extracts the id from a string of a mag entity.

    Args:
        entity (str): the entity url from MAG

    Returns:
        (int): the id of the entity
    """
    rex = r'.+/(\d+)$'
    match = re.match(rex, entity)
    if match is None:
        raise ValueError(f"Unable to extract id from {entity}")
    return int(match.groups()[0])


def query_fields_of_study_sparql(ids=None, results_limit=None):
    """Queries the MAG for fields of study. Expect >650k results for all levels.

    Args:
        ids: (:obj:`list` of `int`): field of study ids to query,
                                     all are returned if None
        results_limit (int): limit the number of results returned (for testing)

    Returns:
        (dict): processed field of study
    """
    for count, row in enumerate(_batch_query_entities(ids)):
        # reformat field, parents, children out of urls.
        row['id'] = extract_entity_id(row.pop('field'))
        row['parent_ids'] = {extract_entity_id(entity)
                             for entity in row.pop('parents').split(',')}
        row['child_ids'] = {extract_entity_id(entity)
                            for entity in row.pop('children').split(',')}
        yield row

        if not count % 1000:
            logging.info(count)

        if results_limit is not None and count >= results_limit:
            logging.warning(f"Breaking after {results_limit} for testing")
            break


def update_field_of_study_ids_sparql(session, fos_ids):
    logging.info(f"Missing field of study ids: {fos_ids}")
    logging.info(f"Querying MAG for {len(fos_ids)} missing fields of study")
    new_fos_to_import = [FieldOfStudy(**fos)
                         for batch in query_fields_of_study_sparql(fos_ids)
                         for fos in batch]
    logging.info(f"Retrieved {len(new_fos_to_import)} new fields of study from MAG")
    fos_not_found = fos_ids - {fos.id for fos in new_fos_to_import}
    if fos_not_found:
        raise ValueError(f"Fields of study present in articles but could not be found in MAG Fields of Study database. New links cannot be created until this is resolved: {fos_not_found}")
    session.add_all(new_fos_to_import)
    session.commit()
    logging.info("Added new fields of study to database")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # setup database connectors
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
