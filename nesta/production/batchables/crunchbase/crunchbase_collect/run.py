import boto3
import logging
import os
import pandas as pd
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


def run():
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    table_name = os.environ["BATCHPAR_table_name"]
    # logging.warning(f"Retrieving {batch_size} articles between {start_cursor - 1}:{end_cursor - 1}")

    # Setup the database connectors
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)

    with crunchbase_tar() as tar:
        df = pd.read_csv(tar.extractfile(''.join([table_name, '.csv'])))


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

