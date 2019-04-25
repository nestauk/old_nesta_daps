from alphabet_detector import AlphabetDetector
from collections import defaultdict
import logging
import pandas as pd
import requests

from nesta.production.luigihacks import misctools
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.mag_orm import FieldOfStudy


def prepare_title(title):
    """Replaces non-alphanums from a paper title, allowing foreign characters and cleans
    up multiple spaces and trailing spaces.

    Args:
        title (str): the title of the paper

    Returns:
        (str): cleaned title
    """
    detector = AlphabetDetector()
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
    endpoint = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate"
    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    query = f"{expr}&count={query_count}&offset={offset}&attributes={','.join(fields)}"

    r = requests.post(endpoint, data=query.encode("utf-8"), headers=headers)
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
        query_count (int): number of items to return
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
    fields_to_compact = ['FC', 'FP']

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


if __name__ == "__main__":
    import json

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # collect api key from config
    mag_config = misctools.get_config('mag.config', 'mag')
    subscription_key = mag_config['subscription_key']

    # setup database connectors
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")

    # *** query papers from arxiv titles
    df = pd.read_csv("/Users/russellwinch/Documents/data/arxiv_2017.csv", nrows=1000)

    author_mapping = {'AuN': 'author_name',
                      'AuId': 'author_id',
                      'AfN': 'author_affiliation',
                      'AfId': 'author_affiliation_id',
                      'S': 'author_order'}

    field_mapping = {"Id": 'id',
                     "Ti": 'title', # not needed
                     "F": 'fields_of_study',
                     "AA": 'authors',
                     "CC": 'citation_count'}
    # query papers
    paper_fields = ["Id", "Ti", "F.FId", "CC", "AA.AuN", "AA.AuId",
                    "AA.AfN", "AA.AfId", "AA.S"]
    for expr in build_expr(list(df.title.apply(prepare_title)), 'Ti', max_length=16000):  # this .apply will take forever on the whole dataset. move to a generator
        # print(expr)
        data = query_mag_api(expr, paper_fields, subscription_key)
        # print(json.dumps(data['entities'][0], indent=4))
        print(f"query length: {len(expr)}")
        print(f"titles in query: {len(expr.split(','))}")
        print(f"entities returned from api: {len(data['entities'])}")

        break

    # for row in data['entities']:
    #     # clean up authors
    #     for author in row['AA']:
    #         for code, description in author_mapping.items():
    #             try:
    #                 author[description] = author.pop(code)
    #             except KeyError:
    #                 pass

    #     # convert fields of study to a list
    #     # row['F'] = [f['FId'] for f in row['F']]

    #     # rename fields
    #     for code, description in field_mapping.items():
    #         try:
    #             row[description] = row.pop(code)
    #         except KeyError:
    #             pass


    # dupe_title = ['convergence of the discrete dipole approximation i theoretical analysis']
    # dupe_title = ['convergence of the discrete dipole approximation ii an extrapolation technique to increase the accuracy']
    # for expr in build_expr(dupe_title, 'Ti'):
    #     # print(expr)
    #     data = query_mag_api(expr, paper_fields, subscription_key)
    #     print(json.dumps(data['entities'][0], indent=4))

    #     break
    # *** json to sql
    # write_fields_of_study_to_db('mag_fields_of_study.json', 'dev')
    # write_fields_of_study_to_db('mag_fields_of_study.json', 'production')


    # *** extract field ids from papers
    # fids = set()
    # for entity in data['entities']:
    #     for f in entity['F']:
    #         fids.add(f['FId'])
    # print(fids)

    # query field ids
    # fos_fields = ['Id', 'DFN', 'FL', 'FP.FN']
    # for expr in build_expr(fids, 'Id'):
    #     # print(expr)
    #     fos_data = query_mag_api(expr, fos_fields)
    #     print(fos_data)
    #     break


    # *** extract list of ids
    # fos_level_fields = ['Id', 'DFN', 'FL', 'FP.FId', 'FC.FId']  # id, display_name, level, parent_ids, children_ids
    # for expr in build_expr([2909385909, 2911099694], 'Id'):
    #     print(expr)
    #     count = 1000
    #     data = query_mag_api(expr, fos_level_fields, query_count=count)
    #     print(data)



    # *** partially completed data science approach to determining levels
    # from sklearn.feature_extraction.text import CountVectorizer

    # corpus = []
    # for row in data["entities"]:
    #     fields = " ".join(fos["DFN"].replace(" ", "_")
    #                       for fos in row["F"])
    #     corpus.append(fields)
    # cv = CountVectorizer()
    # _data = cv.fit_transform(corpus)

    # import numpy as np

    # _, n_fields = _data.shape
    # features = cv.get_feature_names()
    # threshold = 2
    # for i_field in range(0, n_fields):
    #     if i_field != 111:
    #         continue
    #     print(features[i_field])
    #     condition = _data[:, i_field] > 0
    #     condition = np.hstack(condition.toarray())
    #     cooc = np.array(_data[condition].sum(axis=0))[0]
    #     for name, count in zip(features, cooc):
    #         if count >= threshold:
    #             print(name, count)

    # determine threshold (change to coocurrence fraction)
    # e.g. AI occurs in 10 documents, CompSci occurs in 1000,
    # but there are 9 AI docs which also have CompSci, so remove CompSci.
    # The fraction threshold is 90% in this case, and the min count is at least 1000.
    # Bonus: Could also require that a minimum number of "Fields of Study"
