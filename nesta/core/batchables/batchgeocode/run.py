"""
run.py (batch_geocode)
======================

Geocode any row-delimited json data, with columns corresponding 
to a city/town/etc and country.
"""

import logging
import os
import pandas as pd
import s3fs  # not called but required import to read from s3://

from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe
from nesta.packages.geo_utils.geocode import geocode_batch_dataframe
from nesta.core.orms.geographic_orm import Geographic
from nesta.core.orms.orm_utils import db_session, get_mysql_engine


def run():
    batch_file = os.environ['BATCHPAR_batch_file']
    db = os.environ['BATCHPAR_db_name']
    bucket = os.environ['BATCHPAR_bucket']

    # database setup
    engine = get_mysql_engine('BATCHPAR_config', 'mysqldb', db)

    # collect data
    target = f"s3://{bucket}/{batch_file}"
    df = pd.read_json(target, orient='records')
    logging.info(f"{len(df)} locations to geocode")

    # append country iso codes and continent
    df = country_iso_code_dataframe(df)
    logging.info("Country ISO codes appended")

    # geocode, appending latitude and longitude columns, using the q= query method
    df = geocode_batch_dataframe(df, query_method='query_only')
    logging.info("Geocoding complete")

    # remove city and country columns and append done column
    df = df.drop(['city', 'country'], axis=1)
    df['done'] = True

    # convert to list of dict and output to database
    rows = df.to_dict(orient='records')

    logging.info(f"Writing {len(rows)} rows to database")
    with db_session(engine) as session:
        session.bulk_update_mappings(Geographic, rows)
    logging.warning("Batch task complete")


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    if False:
        environ = {"BATCHPAR_done": "False",
                   "BATCHPAR_batch_file" : "geocoding_batch_15597590150867765.json",
                   "BATCHPAR_config": "/home/ec2-user/nesta/nesta/core/config/mysqldb.config",
                   "BATCHPAR_bucket": "nesta-production-intermediate",
                   "BATCHPAR_test": "True",
                   "BATCHPAR_db_name": "dev"}
        for k, v in environ.items():
            os.environ[k] = v 
    run()
