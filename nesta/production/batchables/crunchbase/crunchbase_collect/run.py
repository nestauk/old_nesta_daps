from ast import literal_eval
import boto3
import logging
import os
# import pandas as pd
# import pycountry
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.orm.exc import NoResultFound
from urllib.parse import urlsplit

# from nesta.packages.crunchbase.crunchbase_collect import crunchbase_tar
# from nesta.packages.geo_utils.geocode import geocode_dataframe
# from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe
from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_orgs
from nesta.packages.crunchbase.crunchbase_collect import _insert_data, rename_uuid_columns
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data, get_class_by_tablename
from nesta.production.orms.crunchbase_orm import Base


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    table = os.environ["BATCHPAR_table"]
    logging.info(f"Processing {table} file")

    # database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)
    table_name = f"crunchbase_{table}"
    table_class = get_class_by_tablename(Base, table_name)

    # collect file
    df = get_files_from_tar(table, test=test)

    df = rename_uuid_columns(df)

    # TODO:
    # if city and country convert the country code to name and add composite_key
    # convert any t,f to bools


    # **** old stuff ******
    # if {'city', 'country_code'}.issubset(df.columns):
    #     df['country'] = df['country_code'].apply(_country_name)
    #     df = geocode_dataframe(df)
    #     df = country_iso_code_dataframe(df)
    #     df = df.drop('country_code', axis=1)  # now redundant with country_alpha_3 appended

    # rows = df.to_dict(orient='records')

    # returned = {}
    # returned['inserted'], returned['existing'], returned['failed'] = insert_data(
    #                                             "BATCHPAR_config", "mysqldb", db_name,
    #                                             Base, table_class, rows,
    #                                             return_non_inserted=True)
    # totals = _total_records(returned)
    # for k, v in totals:
    #     logging.warning(f"{k} rows: {v}")

    # # check before the task is marked as done
    # if totals['total'] != len(df):
    #     raise ValueError("Inserted rows do not match existing data")

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

