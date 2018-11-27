import boto3
import logging
import pandas as pd
import s3fs

from nesta.packages.geo_utils.geocode import geocode_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe


def collect_file_from_s3(bucket, filepath, columns=None):
    """Retrieves a file from s3 and loads it into a dataframe.

    Args:
        bucket (str): name of the s3 bucket
        filepath (str): path to the file within the bucket
        columns (list): list of columns to keep, if None, all are kept

    Returns:
        :code:`pd.DataFrame` containing the whole csv file
    """
    target = f"s3://{bucket}/{filepath}"
    # logging.debug(f"Retrieving mesh terms from S3: {target}")

    return pd.read_csv(target, usecols=columns)


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    log_file_handler = logging.FileHandler('logs.log')
    logging.basicConfig(handlers=(log_stream_handler, log_file_handler),
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    bucket = 'crunchbase-export'
    target = 'organizations.csv'
    # org_columns = [
