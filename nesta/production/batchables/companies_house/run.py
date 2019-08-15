import logging
import os
import json
import asyncio
from ast import literal_eval
import boto3
import sqlalchemy
from nesta.packages.companies_house.find_dissolved import dispatcher
from nesta.production.luigihacks.s3 import parse_s3_path
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, db_session
from nesta.production.orms.companies_house_orm import Base, DiscoveredCompany


def run():
    """ Batch component: Queries Companies House API for company numbers"""
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]

    s3_input = os.environ["BATCHPAR_inputinfo"]
    api_key = os.environ["BATCHPAR_CH_API_KEY"]

    # Read candidates from S3 input
    s3 = boto3.resource('s3')
    logging.info(parse_s3_path(s3_input))
    s3_obj = s3.Object(*parse_s3_path(s3_input))
    candidates = json.loads(s3_obj.get()['Body'].read())

    if test:
        candidates = candidates[:10]
        logging.info(candidates)

    logging.info(f"Using {db_name} database")
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # Create table if it doesn't exist - TODO put this in prepare
    if not engine.dialect.has_table(engine, "ch_discovered"):
        DiscoveredCompany.__table__.create(engine)
        logging.info(f"Table {DiscoveredCompany.__table__} created")

    def consume(r):
        """ Enter row into database """
        success, row = r
        logging.debug(row)
        if row['status'] != 404:
            engine.execute(
                    "REPLACE INTO ch_discovered "
                    "(company_number, response) "
                    "VALUES (%s, %s)",
                    (row['company_number'], int(row['status']))
                    )
        else:
            logging.debug(f"404: {row['company_number']} does not exist")

    # Make requests
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
            dispatcher(
                candidates,
                api_key,
                consume=consume,
                ratelim=(API_CALLS, API_TIME)
                )
        )

    # TODO Re-run on non 404 or 200 status codes

    logging.info(f"Marking task as done to {s3_path}")
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")

if __name__ == "__main__":
    if "BATCHPAR_outinfo" not in os.environ.keys():  # Local testing
        os.environ["BATCHPAR_CH_API_KEY"] = "EKiJ7O-o-j_6zHuFogXiwOO5VuVYnrnAI8rFSY5N"
        os.environ["BATCHPAR_db_name"] = "dev"
        os.environ["BATCHPAR_inputinfo"] = "s3://nesta-production-intermediate/CH_batch_params_EKiJ7O-o-j_6zHuFogXiwOO5VuVYnrnAI8rFSY5N_input"
        os.environ["BATCHPAR_outinfo"] = "s3://nesta-production-intermediate/local_out"
        os.environ["BATCHPAR_done"] = "False"
        os.environ["BATCHPAR_test"] = "True"
        os.environ["BATCHPAR_config"] = os.environ["MYSQLDB"]
        level = logging.DEBUG
    else:
        level = logging.INFO

    API_CALLS, API_TIME = 600, 300
    key = os.environ["BATCHPAR_CH_API_KEY"]

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler],
                       level=level,
                       format="%(asctime)s:%(levelname)s:%(message)s")
    run()
