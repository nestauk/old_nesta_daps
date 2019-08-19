from bs4 import BeautifulSoup
from collections import defaultdict
from jellyfish import levenshtein_distance
import json
import logging
import re
import requests

from nesta.packages.misc_utils.batches import split_batches
from nesta.packages.misc_utils.sparql_query import sparql_query
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.mag_orm import FieldOfStudy


MAG_ENDPOINT = 'http://ma-graph.org/sparql'


def _batch_query_articles_by_doi(query, articles, batch_size=10):
    """Manages batches and generates sparql queries for articles and queries them from
    mag via the sparql api using the supplied `doi`.

    Args:
        query (str): sparql query containing a format string placeholder {}
        articles (:obj:`list` of :obj:`dict`): articles to query in MAG.
            Must contatin at least `id` and `doi` in each dict.
        batch_size (int): number of ids to query in a batch. Max size = 50

    Yields:
        (:obj:`list` of :obj:`dict`): batches of data returned from MAG
    """
    if not 1 <= batch_size <= 10:  # max limit for uri length
        raise ValueError("batch_size must be between 1 and 10")

    for articles_batch in split_batches(articles, batch_size):
        clean_dois = [(a['doi']
                       .replace('\n', '')
                       .replace('\\', '')
                       .replace('"', '')) for a in articles_batch]
        concat_dois = ','.join(f'"{a}"^^xsd:string' for a in clean_dois)
        article_filter = f"FILTER (?doi IN ({concat_dois}))"

        for results_batch in sparql_query(MAG_ENDPOINT,
                                          query.format(article_filter=article_filter)):
            yield articles_batch, results_batch


def query_articles_by_doi(articles):
    """Queries Microsoft Academic Graph via the SPARQL endpoint, using doi.
    Deduplication is applied by identifying the closest match on title.

    Args:
        articles (:obj:`list` of :obj:`dict`): articles to query in MAG.
            Must contatin at least `id` and `doi` in each dict.

    Yields:
        (dict): single article
    """
    query = '''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX datacite: <http://purl.org/spar/datacite/>
    PREFIX fabio: <http://purl.org/spar/fabio/>
    PREFIX magp: <http://ma-graph.org/property/>

    SELECT ?paper
           ?doi
           ?paperTitle
           ?citationCount
           GROUP_CONCAT(DISTINCT ?fieldOfStudy; separator=",") as ?fieldsOfStudy
           GROUP_CONCAT(DISTINCT ?author; separator=",") as ?authors
    WHERE {{
        ?paper datacite:doi ?doi .
        ?paper magp:citationCount ?citationCount .
        ?paper dcterms:title ?paperTitle .
        ?paper magp:citationCount ?citationCount .
        ?paper fabio:hasDiscipline ?fieldOfStudy .
        ?paper dcterms:creator ?author .
        {article_filter}
    }}
    GROUP BY ?paper ?doi ?paperTitle ?citationCount
    ORDER BY ?paper
    '''
    for articles_batch, results_batch in _batch_query_articles_by_doi(query, articles):
        # combine results by doi
        articles_to_dedupe = defaultdict(list)
        for result in results_batch:
            # duplicate dois exist in response, eg: '10.1103/PhysRevD.76.052005'
            articles_to_dedupe[result['doi']].append(result)

        for article in articles_batch:
            # has to be .get here as defaultdict creates new entries for failed lookups
            found_articles = articles_to_dedupe.get(article['doi'])
            if found_articles is None:
                # no matches
                continue

            # calculate the score for difference between the titles
            for found_article in found_articles:
                try:
                    found_article['score'] = levenshtein_distance(found_article['paperTitle'],
                                                                  article['title'])
                except KeyError:
                    # hopefully the last possible match
                    found_article['score'] = 9999

            # determine the closest title match by score
            best_match = sorted(found_articles, key=lambda x: x['score'])[0]
            best_match['id'] = article['id']

            yield best_match


def _batched_entity_filter(concat_format, filter_on, ids, batch_size):
    """Creates batches of entity filters for SPARQL queries. Constructing a
    'filter in' statement using the provided ids, splitting whenever the batch size is
    hit.

    A call with the following arguments:
        _batched_entity_filter('<http://ma-graph.org/entity/{}>', 'data', [1, 2], 50)

    Will return a generator which yields the following:
        "FILTER (?data IN (<http://ma-graph.org/entity/1>,<http://ma-graph.org/entity/2>))"

    Args:
        concat_format (str): string format to be applied when concatenating ids
            requires a placeholder for id {}
        filter_on (str): name of the field to use in the filter. The '?' prefix
            is not required
        ids (list): If ids are supplied they are queried as batches, otherwise
            all entities are queried
        batch_size (int): number of ids to query in a batch.

    Yields:
        (str): filter string containing ids up to the chosen batch size
    """
    for batch_of_ids in split_batches(ids, batch_size):
        entities = ','.join(concat_format.format(i) for i in batch_of_ids)

        yield f"FILTER (?{filter_on} IN ({entities}))"


def _batch_query_sparql(query,
                        concat_format=None, filter_on=None, ids=None,
                        batch_size=50):
    """Manages batching of sparql queries, with filtering and yielding of single rows
    mag via the sparql api.

    Args:
        query (str): sparql query containing a format string placeholder {}
        concat_format (str): string format to be applied when concatenating ids
            requires a placeholder for id {}
        filter_on (str): name of the field to use in the filter. The '?' prefix
            is not required
        ids (list): If ids are supplied they are queried as batches, otherwise
            all entities are queried
        batch_size (int): number of ids to query in a batch. Maximum = 50

    Yields:
        (dict): single row of returned data
    """
    if not 1 <= batch_size <= 50:  # max limit for uri length
        raise ValueError("batch_size must be between 1 and 50")

    if all([concat_format, filter_on, ids]):
        entity_filters = _batched_entity_filter(concat_format, filter_on, ids,
                                                batch_size)
    elif any([concat_format, filter_on, ids]):
        raise ValueError("concat_format, filter_on and ids must all be supplied "
                         "together or not at all")
    else:
        # retrieve all
        entity_filters = ['']

    for entity_filter in entity_filters:
        for batch in sparql_query(MAG_ENDPOINT, query.format(entity_filter)):
            yield from batch


def extract_entity_id(entity):
    """Extracts the id from the end of an entity url returned from sparql.

    Args:
        entity (str): the entity url from MAG

    Returns:
        (int or str): the id of the entity
    """
    rex = r'.+/(.+)$'  # capture anything after the last /
    match = re.match(rex, entity)
    try:
        return int(match.groups()[0])
    except ValueError:
        return match.groups()[0]
    except AttributeError:
        raise ValueError(f"Unable to extract id from {entity}")


def query_fields_of_study_sparql(ids=None, results_limit=None):
    """Queries the MAG for fields of study. Expect >650k results for all levels.

    Args:
        ids: (:obj:`list` of `int`): field of study ids to query,
                                     all are returned if None
        results_limit (int): limit the number of results returned (for testing)

    Yields:
        (dict): processed field of study
    """
    query = '''
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX magc: <http://ma-graph.org/class/>
    PREFIX magp: <http://ma-graph.org/property/>

    SELECT ?field
           ?name
           ?level
           GROUP_CONCAT(DISTINCT ?parent; separator=",") as ?parents
           GROUP_CONCAT(?child; separator=",") as ?children
    WHERE {{
        ?field rdf:type magc:FieldOfStudy .
        ?field magp:level ?level .
        OPTIONAL {{ ?field foaf:name ?name }}
        OPTIONAL {{ ?field magp:hasParent ?parent }}
        OPTIONAL {{ ?child magp:hasParent ?field }}
        {}
    }}
    GROUP BY ?field ?name ?level'''
    concat_format = "<http://ma-graph.org/entity/{}>"

    for count, row in enumerate(_batch_query_sparql(query,
                                                    concat_format=concat_format,
                                                    filter_on='field',
                                                    ids=ids), start=1):
        # reformat field, parents, children out of urls.
        row['id'] = extract_entity_id(row.pop('field'))

        parents = row.pop('parents')
        if parents == '':
            row['parent_ids'] = None
        else:
            row['parent_ids'] = ','.join(str(extract_entity_id(entity))
                                         for entity in parents.split(','))

        children = row.pop('children')
        if children == '':
            row['child_ids'] = None
        else:
            # adding a DISTINCT for children made the query incredibly slow, hence the extra set here
            row['child_ids'] = ','.join({str(extract_entity_id(entity))
                                         for entity in children.split(',')})

        yield row

        if not count % 1000:
            logging.info(count)

        if results_limit is not None and count >= results_limit:
            logging.warning(f"Breaking after {results_limit} for testing")
            break


def update_field_of_study_ids_sparql(engine, fos_ids):
    """Queries MAG via the sparql api for fields of study and if found, adds them to the
    database. Only ids of missing fields of study should be supplied, no check is done
    here to determine if it already exists.

    Args:
        engine (:obj:`sqlalchemy.engine`): database connection
        fos_ids (list): ids to search and update

    Returns:
        (set): ids which could not be found in MAG
    """
    logging.info(f"Querying MAG for {len(fos_ids)} missing fields of study")
    new_fos_to_import = [FieldOfStudy(**fos)
                         for fos in query_fields_of_study_sparql(fos_ids)]

    logging.info(f"Retrieved {len(new_fos_to_import)} new fields of study from MAG")
    fos_not_found = set(fos_ids) - {fos.id for fos in new_fos_to_import}
    if fos_not_found:
        logging.warning(f"Fields of study present in articles but could not be found in MAG Fields of Study database: {fos_not_found}")
    with db_session(engine) as session:
        session.add_all(new_fos_to_import)
        session.commit()
    logging.info("Added new fields of study to database")
    return fos_not_found


def query_authors(ids=None, results_limit=None):
    """Queries the MAG for authors and their affiliations.

    Args:
        ids: (:obj:`list` of `int`): author ids to query, all are returned if None
        results_limit (int): limit the number of results returned (for testing)

    Yields:
        (dict): a single author with affiliation
    """
    query = '''
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX org: <http://www.w3.org/ns/org#>
    PREFIX magp: <http://ma-graph.org/property/>
    PREFIX magc: <http://ma-graph.org/class/>

    SELECT ?author
           ?authorName
           ?affiliation
           ?affiliationName
           ?gridId
    WHERE {{
        ?author rdf:type magc:Author .
        ?author foaf:name ?authorName .
        ?author org:memberOf ?affiliation .
        ?affiliation magp:grid ?gridId .
        ?affiliation foaf:name ?affiliationName .
        {}
    }}
    '''
    concat_format = "<http://ma-graph.org/entity/{}>"

    logging.debug(f"Querying MAG for authors: {ids}")
    for count, row in enumerate(_batch_query_sparql(query,
                                                    concat_format=concat_format,
                                                    filter_on='author',
                                                    ids=ids), start=1):
        renaming = {'author': 'author_id',
                    'authorName': 'author_name',
                    'affiliation': 'author_affiliation_id',
                    'affiliationName': 'author_affiliation',
                    'gridId': 'affiliation_grid_id'}

        for old, new in renaming.items():
            if new.endswith('_id'):
                row[new] = extract_entity_id(row.pop(old))
            else:
                row[new] = row.pop(old)

        yield row

        if not count % 1000:
            logging.info(count)

        if results_limit is not None and count >= results_limit:
            logging.warning(f"Breaking after {results_limit} for testing")
            break


def check_institute_exists(grid_id):
    query = '''
    PREFIX magc: <http://ma-graph.org/class/>
    PREFIX magp: <http://ma-graph.org/property/>

    SELECT ?affiliationId

    WHERE {{
    ?affiliationId rdf:type magc:Affiliation .
    ?affiliationId magp:grid ?affiliationGridId .

    FILTER (?affiliationGridId = <http://www.grid.ac/institutes/{grid_id}>)
    }}
    '''
    logging.debug(f"Checking {grid_id} exists in MAG")
    return list(sparql_query(MAG_ENDPOINT, query.format(grid_id=grid_id))) != []


def query_by_grid_id(grid_id, from_date='2000-01-01', min_citations=1,
                     batch_size=5000, batch_limit=None):
    """Queries the MAG for authors, papers, fields of study from a grid id.

    Args:
        ids: (:obj:`list` of `int`): author ids to query, all are returned if None
        from_date (str): minimum date for paper created date in YYYY-MM-DD format
        min_citations (int): minimum number of citations a paper has received
        batch_size (int): number of rows to retrieve when batching calls to MAG
        results_limit (int): limit the number of results returned, for testing

    Yields:
        (dict): yields a row of returned data
    """
    query = '''
    PREFIX magc: <http://ma-graph.org/class/>
    PREFIX grid: <http://www.grid.ac/institutes>
    PREFIX magp: <http://ma-graph.org/property/>
    PREFIX org: <http://www.w3.org/ns/org#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX datacite: <http://purl.org/spar/datacite/>
    PREFIX fabio: <http://purl.org/spar/fabio/>

    SELECT
        ?authorId
        ?authorName
        ?paperId
        ?paperTitle
        ?paperCitationCount
        ?paperCreatedDate
        ?paperDoi
        ?paperLanguage
        ?bookTitle
        ?fieldOfStudyId

    WHERE {{
        ?affiliationId rdf:type magc:Affiliation .
        ?affiliationId magp:grid ?affiliationGridId .
        ?authorId org:memberOf ?affiliationId .
        ?authorId foaf:name ?authorName .
        ?paperId dcterms:creator ?authorId .
        ?paperId dcterms:title ?paperTitle .
        ?paperId magp:citationCount ?paperCitationCount .
        ?paperId dcterms:created ?paperCreatedDate .
        OPTIONAL {{?paperId datacite:doi ?paperDoi .}}
        OPTIONAL {{?paperId dcterms:language ?paperLanguage .}}
        OPTIONAL {{?paperId magp:bookTitle ?bookTitle .}}
        ?paperId fabio:hasDiscipline ?fieldOfStudyId .

        FILTER (?paperCreatedDate >= "{from_date}"^^xsd:date)
        FILTER (?paperCitationCount >= {min_citations})
        FILTER (?affiliationGridId = <http://www.grid.ac/institutes/{grid_id}>)
    }}
    '''
    logging.debug(f"Querying MAG for institute {grid_id}")

    for batch in (sparql_query(MAG_ENDPOINT,
                               nbatch=batch_size,
                               batch_limit=batch_limit,
                               query=query.format(grid_id=grid_id,
                                                  from_date=from_date,
                                                  min_citations=min_citations))):
        for row in batch:
            # extract any of ids
            yield {k: extract_entity_id(v) if k.endswith('Id') else v
                   for k, v in row.items()}


def batch_query_papers_by_institute_id(query):
    """Queries MAG for a batch and yields a row of data at a time.

    Args:
        query(str): SPARQL query

    Yields:
        (dict): row of data
    """
    for batch in sparql_query(MAG_ENDPOINT, query):
        yield from batch

def count_papers(institutes, done_institutes, paper_ids, intermediate_file,
                 save_every=1000000, limit=None):
    """Count the number of papers produced from a list of supplied institues.

    Args:
        institutes(list): GRID IDs of the institutes to look up
        done_institutes(list): GRID IDs of institues which have already been processed
        paper_ids(list): MAG IDs of papers which have already been processed
        intermediate_file(:obj:`boto3.resources.s3.object`): file to store processed
        institutes and paper ids
        save_every(int): saves to s3 each time this number of papers are found
        limit(int): break at this number of processed records, for testing

    Returns:
        (int): total number of papers found
    """
    query = '''
    PREFIX magp: <http://ma-graph.org/property/>
    PREFIX org: <http://www.w3.org/ns/org#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?paperId

    WHERE {{
    ?affiliationId magp:grid <http://www.grid.ac/institutes/{}> .
    ?authorId org:memberOf ?affiliationId .
    ?paperId dcterms:creator ?authorId .
    }}
    '''
    count = 0
    for institute in institutes:
        for row in batch_query_papers_by_institute_id(query.format(institute)):
            count += 1
            logging.debug(row)
            paper_id = extract_entity_id(row['paperId'])
            if paper_id in paper_ids:
                continue
            paper_ids.add(paper_id)
            total = len(paper_ids)
            if not total % 1000:
                logging.info(f"{total:,} EU papers found")

            if not total % save_every:
                # write out to file on S3 for safekeeping
                logging.info("Writing out to file with "
                             f"{len(done_institutes)} institutes "
                             f"and {total} papers")
                intermediate_file.put(Body=json.dumps({'paper_ids': list(paper_ids),
                                                       'institutes': list(done_institutes)}))

            if limit is not None and count >= limit:
                logging.warning(f"Breaking after {limit} papers in test mode")
                return limit

        done_institutes.add(institute)

    return len(paper_ids)


def get_eu_countries():
    """Retrieves names of EU countries from online service and parses them.

    Returns:
        (:obj:`list` of :obj:`str`): names of all the countries in the EU
    """
    country_endpoint = "https://europa.eu/european-union/about-eu/countries_en"

    response = requests.get(country_endpoint)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, features='lxml')
    table = soup.find(id='year-entry2')

    return [country.string for country in table.find_all('a')]


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # setup database connectors
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
