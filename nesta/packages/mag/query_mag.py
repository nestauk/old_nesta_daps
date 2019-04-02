from alphabet_detector import AlphabetDetector
import logging
import pandas as pd
import requests

from nesta.production.luigihacks import misctools
from nesta.production.orms.orm_utils import get_mysql_engine


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


def build_expr(query_items, entity_name, max_length=1000):
    """Builds and yields OR expressions for MAG from a list of items. Strings and
    integer items are formatted quoted and unquoted respectively, as per the MAG query
    specification.

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
    for item in query_items:
        length += len(str(item))
        if length > max_length:
            yield f"expr=OR({','.join(expr)})"
            expr = []
            length = len(str(item))
        if type(item) == str:
            expr.append(f"{entity_name}='{item}'")
        elif type(item) == int:
            expr.append(f"{entity_name}={item}")

    yield f"expr=OR({','.join(expr)})"


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
                          levels=[0, 1, 2, 3, 4, 5],
                          fields=['Id', 'DFN', 'FL', 'FP.FId', 'FC.FId'],  # id, display_name, level, parent_ids, children_ids
                          query_count=1000,
                          results_limit=None):
    """Queries the MAG for fields of study. Expect >650k results for all levels.

    Args:
        subscription_key (str): MAG api subscription key
        levels (:obj:`list` of `int`): levels to extract. 0 is the highest, 5 is lowest
        fields (:obj:`list` of `str`): codes of fields to return, as per mag documentation
        query_count (int): number of items to return
        results_limit (int): break and return as close to this number of results as the
                             offset and query_count allow (for testing)

    Returns:
        (:obj:`list` of `dict`): results from the api query
    """
    fields_of_study = []
    for expr in build_expr(levels, 'FL'):
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
            fields_to_drop = ['logprob', 'prob']
            fields_to_compact = ['FC', 'FP']
            for row in fos_data['entities']:
                # remove unnecessary fields
                for f in fields_to_drop:
                    del row[f]
                for c in fields_to_compact:
                    try:
                        row[c] = [ids['FId'] for ids in row[c]]
                    except KeyError:
                        # no parents and/or children
                        pass
                fields_of_study.append(row)

            offset += len(fos_data['entities'])
            logging.info(offset)
            if results_limit is not None and offset >= results_limit:
                return fields_of_study

    return fields_of_study


def concatenate_ids(ids):
    """Converts a list of ids into a comma separated string.
    Args:
        ids (list or other iterable)

    Returns:
        (str): comma seperated string
    """
    if ids is not pd.np.nan:
        return ','.join(str(i) for i in ids)


def write_fields_of_study_to_db(data, engine, chunksize=10000):
    """Writes fields of study to a database.

    Args:
        data (:obj:`list` of `dict`): rows of fields of study data
        engine (:obj:`sqlalchemy.engine.base.Engine`): connection to the database
        chunksize (int): number of rows to write in each insert
    """
    logging.warning(f"Using {engine.url.database} database")

    # convert to a dataframe
    df = pd.DataFrame(data)

    logging.info("Renaming columns and cleaning data")
    column_remapping = {'DFN': 'name',
                        'FC': 'children_ids',
                        'FL': 'level',
                        'FP': 'parent_ids',
                        'Id': 'id'}
    df = df.rename(columns=column_remapping)
    df.children_ids = df.children_ids.apply(concatenate_ids)
    df.parent_ids = df.parent_ids.apply(concatenate_ids)

    # write to sql
    logging.info(f"Writing {len(df)} rows to database")
    df.to_sql('mag_fields_of_study', con=engine, index=False,
              if_exists='append', chunksize=chunksize)
    logging.info("Writing to database complete")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # collect api key from config
    mag_config = misctools.get_config('nesta/production/config/mag.config', 'mag')
    subscription_key = mag_config['subscription_key']

    # setup database connectors
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")

    # *** query papers from arxiv titles
    # df = pd.read_csv("/Users/russellwinch/Documents/data/arxiv_2017.csv", nrows=1000)

    # author_mapping = {'AuN': 'author_name',
    #                   'AuId': 'author_id',
    #                   'AfN': 'author_affiliation',
    #                   'AfId': 'author_affiliation_id',
    #                   'S': 'author_order'}
    # # query papers
    # paper_fields = ["Id", "Ti", "F.DFN", "F.FId", "CC", "AA.AuN", "AA.AuId",
    #                 "AA.AfN", "AA.AfId", "AA.S"]
    # for expr in build_expr(list(df.title.apply(prepare_title)), 'Ti'):  # this .apply will take forever on the whole dataset. move to a generator
    #     # print(expr)
    #     data = query_mag_api(expr, paper_fields)
    #     print(json.dumps(data['entities'][0], indent=4))
    #     break

    #     # clean up authors
    #     for row in data['entities']:
    #         for author in row['AA']:
    #             for code, description in author_mapping.items():
    #                 try:
    #                     author[description] = author.pop(code)
    #                 except KeyError:
    #                     pass


    # *** json to sql
    # write_fields_of_study_to_db('mag_fields_of_study.json', 'dev')
    write_fields_of_study_to_db('mag_fields_of_study.json', 'production')


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
