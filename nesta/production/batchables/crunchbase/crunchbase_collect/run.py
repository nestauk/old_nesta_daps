import boto3
import logging
import os
import pandas as pd
import pycountry
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from urllib.parse import urlsplit

from nesta.packages.crunchbase.crunchbase_collect import crunchbase_tar
from nesta.packages.geo_utils.geocode import geocode_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data
from nesta.production.orms.arxiv_orm import Base, Articles, ArticleCategories, Categories


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def _country_name(code):
    """Converts country alpha_3 into name.

    Args:
        code (str): iso alpha 3 code

    Returns:
        str: name of the country
    """
    try:
        return pycountry.countries.get(alpha_3=code).name
    except KeyError:
        return pd.np.nan


def _total_records(data_dict):
    totals = {}
    total = 0
    for k, v in data_dict.items():
        length = len(v)
        totals[k] = length
        total += length
    totals['total'] = total
    return totals


def run():
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    table_name = os.environ["BATCHPAR_table_name"]
    logging.warning(f"Processing {table_name} file")

    # Setup the database connectors
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)

    with crunchbase_tar() as tar:
        df = pd.read_csv(tar.extractfile(''.join([table_name, '.csv'])), low_memory=False)

    df.rename(columns={'uuid': 'id'})

    if {'city', 'country_code'}.issubset(df.columns):
        df['country'] = df['country_code'].apply(_country_name)
        df = geocode_dataframe(df)
        df = country_iso_code_dataframe(df)
        df = df.drop('country_code', axis=1)  # now redundant with country_alpha_3 appended

    rows = df.to_dict(orient='records')

    returned = {}
    returned['inserted'], returned['existing'], returned['failed'] = insert_data(
                                                "BATCHPAR_config", "mysqldb", db_name,
                                                Base, table_name, rows,
                                                return_non_inserted=True)
    totals = _total_records(returned)
    for k, v in totals:
        logging.warning(f"{k} rows: {v}")

    # check before the task is marked as done
    if totals['total'] != len(df):
        raise ValueError("Inserted rows do not match existing data")

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()

