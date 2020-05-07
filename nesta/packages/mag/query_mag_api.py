from alphabet_detector import AlphabetDetector
from collections import defaultdict
import logging
import pandas as pd
import requests
from retrying import retry

from nesta.core.luigihacks import misctools
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.mag_orm import FieldOfStudy
from nesta.packages.date_utils.date_utils import weekchunks

ENDPOINT = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate"
STANDARD_FIELDS = ["Id", "Ti", "AA.AfId", "AA.AfN", "AA.AuId",
                   "AA.DAuN", "AA.S", "CC", "D", "F.DFN",
                   "F.FId", "J.JId", "J.JN", "Pt", "RId",
                   "Y", "DOI", "PB", "BT", "IA", "C.CN",
                   "C.CId", "DN", "S"]


def prepare_title(title):
    """Replaces non-alphanums from a paper title, allowing foreign characters and cleans
    up multiple spaces and trailing spaces.

    Args:
        title (str): the title of the paper

    Returns:
        (str): cleaned title
    """
    detector = AlphabetDetector()
    if title is None:
        return ""
    result = "".join([x if len(detector.detect_alphabet(x)) > 0 or x.isnumeric()
                      else " " for x in title.lower()])
    # Recursively remove spaces
    while "  " in result:
        result = result.replace("  ", " ")
    # Remove trailing spaces
    if result[-1] == " ":
        result = result[0:-1]
    return result


def build_expr(query_items, entity_name, max_length=16000):
    """Builds and yields OR expressions for MAG from a list of items. Strings and
    integer items are formatted quoted and unquoted respectively, as per the MAG query
    specification.

    The maximum accepted query length for the api appears to be around 16,000 characters.

    Args:
        query_items (list): all items to be queried
        entity_name (str): the mag entity to be queried ie 'Ti' or 'Id'
        max_length (int): length of the expression which should not be exceeded. Yields
            occur at or below this expression length

    Returns:
        (str): expression in the format expr=OR(entity_name=item1, entity_name=item2...)
    """
    expr = []
    length = 0
    query_prefix_format = "expr=OR({})"
    for item in query_items:
        if type(item) == str:
            formatted_item = f"{entity_name}='{item}'"
        elif type(item) == int:
            formatted_item = f"{entity_name}={item}"
        length = sum(len(e) + 1 for e in expr) + len(formatted_item) + len(query_prefix_format)
        if length >= max_length:
            yield query_prefix_format.format(','.join(expr))
            expr.clear()
        expr.append(formatted_item)

    # pick up any remainder below max_length
    if len(expr) > 0:
        yield query_prefix_format.format(','.join(expr))


#@retry(stop_max_attempt_number=10)
def query_mag_api(expr, fields, subscription_key, query_count=1000, offset=0):
    """Posts a query to the Microsoft Academic Graph Evaluate API.

    Args:
        expr (str): expression as built by build_expr
        fields: (:obj:`list` of `str`): codes of fields to return, as per mag documentation
        query_count: (int): number of items to return
        offset (int): offset in the results if paging through them

    Returns:
        (dict): json response from the api containing 'expr' (the original expression)
                and 'entities' (the results) keys.
                If there are no results 'entities' is an empty list.
    """
    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    query = f"{expr}&count={query_count}&offset={offset}&attributes={','.join(fields)}"

    r = requests.post(ENDPOINT, data=query.encode("utf-8"), headers=headers)
    r.raise_for_status()

    return r.json()


def query_fields_of_study(subscription_key,
                          ids=None,
                          levels=None,
                          fields=['Id', 'DFN', 'FL', 'FP.FId', 'FC.FId'],
                          # id, display_name, level, parent_ids, children_ids
                          query_count=1000,
                          results_limit=None):
    """Queries the MAG for fields of study. Expect >650k results for all levels.

    Args:
        subscription_key (str): MAG api subscription key
        ids: (:obj:`list` of `int`): field of study ids to query
        levels (:obj:`list` of `int`): levels to extract. 0 is highest, 5 is lowest
        fields (:obj:`list` of `str`): codes of fields to return, as per mag documentation
        query_count (int): number of items to return from each query
        results_limit (int): break and return as close to this number of results as the
                             offset and query_count allow (for testing)

    Returns:
        (:obj:`list` of `dict`): processed results from the api query
    """
    if ids is not None and levels is None:
        expr_args = (ids, 'Id')
    elif levels is not None and ids is None:
        expr_args = (levels, 'FL')
    else:
        raise TypeError("Field of study ids OR levels should be supplied")

    field_mapping = {'Id': 'id',
                     'DFN': 'name',
                     'FL': 'level',
                     'FP': 'parent_ids',
                     'FC': 'child_ids'}
    fields_to_drop = ['logprob', 'prob']
    fields_to_compact = ['parent_ids', 'child_ids']

    for expr in build_expr(*expr_args):
        logging.info(expr)
        count = 1000
        offset = 0
        while True:
            fos_data = query_mag_api(expr, fields, subscription_key=subscription_key,
                                     query_count=count, offset=offset)
            if fos_data['entities'] == []:
                logging.info("Empty entities returned, no more data")
                break

            # clean up and formatting
            for row in fos_data['entities']:
                for f in fields_to_drop:
                    del row[f]

                for code, description in field_mapping.items():
                    try:
                        row[description] = row.pop(code)
                    except KeyError:
                        pass

                for field in fields_to_compact:
                    try:
                        row[field] = ','.join(str(ids['FId']) for ids in row[field])
                    except KeyError:
                        # no parents and/or children
                        pass

                logging.info(f'new fos: {row}')
                yield row

            offset += len(fos_data['entities'])
            logging.info(offset)

            if results_limit is not None and offset >= results_limit:
                break


def dedupe_entities(entities):
    """Finds the highest probability match for each title in returned entities from MAG.
    Args:
        entities (:obj:`list` of `dict`): entities from the MAG api

    Returns:
        (set): ids of entities with the highest probability score, one for each title
    """
    titles = defaultdict(dict)
    for row in entities:
        titles[row['Ti']].update({row['Id']: row['logprob']})

    deduped_mag_ids = set()
    for title in titles.values():
        # find highest probability match for each title
        deduped_mag_ids.add(sorted(title, key=title.get, reverse=True)[0])

    return deduped_mag_ids


def update_field_of_study_ids(mag_subscription_key, session, fos_ids):
    logging.info(f"Missing field of study ids: {fos_ids}")
    logging.info(f"Querying MAG for {len(fos_ids)} missing fields of study")
    new_fos_to_import = [FieldOfStudy(**fos) for fos
                         in query_fields_of_study(mag_subscription_key, ids=fos_ids)]
    logging.info(f"Retrieved {len(new_fos_to_import)} new fields of study from MAG")
    fos_not_found = fos_ids - {fos.id for fos in new_fos_to_import}
    if fos_not_found:
        raise ValueError(f"Fields of study present in articles but could not be found in MAG Fields of Study database. New links cannot be created until this is resolved: {fos_not_found}")

    session.add_all(new_fos_to_import)
    session.commit()
    logging.info("Added new fields of study to database")


def build_composite_expr(query_values, entity_name, date):
    """Builds a composite expression with ANDs in OR to be used as MAG query.
    Args:
        query_values (:obj:`list` of :obj:`str`): Phrases to query MAG with.
        entity_name (str): MAG attribute that will be used in query.
        date (:obj:`tuple` of :obj:`str`): Time period of the data collection.
    Returns:
        (str) MAG expression.
    """
    and_queries = [
        "".join(
            [
                f"AND(Composite({entity_name}='{query_value}'), D=['{date[0]}', '{date[1]}'])"
            ]
        )
        for query_value in query_values
    ]
    # MAG "OR" does not work for single expressions
    if len(and_queries) == 1:
        return f"expr={and_queries[0]}"
    # Join into a MAG "OR" query
    return "expr=OR({})".format(", ".join(and_queries))


def get_journal_articles(journal_name, start_date,
                         until_date=None, until_days_ago=0,
                         api_key=None, fields=STANDARD_FIELDS):
    """Get all articles for a given journal, between two dates.

    Args:
        journal_name (str): The name of the journal to query.
        start (str): Sensibly formatted datestring (format to be guessed by pd)
        until (str): Another datestring. Default=today.
        until_days_ago (str): if until is not specified, this indicates how many days ago to consider. Default=0.
        api_key (str): MAG API key.
        fields (list): List of MAG fields to return from the query.
    Yields:
        article (dict): An article object from MAG.
    """
    for weekchunk in weekchunks(start_date, until_date, until_days_ago):
        expr = build_composite_expr([journal_name],
                                    'J.JN', weekchunk)
        n, offset = None, 0
        while n != 0:
            data = query_mag_api(expr, fields, api_key,
                                 offset=offset)
            for article in data['entities']:
                yield article
            n = len(data['entities'])
            offset += n
