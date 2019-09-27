import datetime
import json
import logging
import os
from ast import literal_eval

import boto3
import sqlalchemy

from nesta.core.luigihacks.s3 import parse_s3_path
from nesta.core.orms.companies_house_orm import Base, DiscoveredCompany
from nesta.core.orms.orm_utils import (db_session, get_mysql_engine,
                                       try_until_allowed)
from nesta.packages.companies_house.find_dissolved import dispatcher


def run():
    """ Batch component: Queries Companies House API for company numbers"""
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]

    s3_input = os.environ["BATCHPAR_inputinfo"]
    s3_interim = os.environ["BATCHPAR_interiminfo"]
    api_key = os.environ["BATCHPAR_CH_API_KEY"]

    # Read candidates from S3 input
    s3 = boto3.resource("s3")
    logging.info(parse_s3_path(s3_input))
    s3_obj = s3.Object(*parse_s3_path(s3_input))
    candidates = json.loads(s3_obj.get()["Body"].read())

    if test:
        candidates = candidates[:300]
        logging.info(candidates)

    logging.info(f"Using {db_name} database")
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # Create table if it doesn't exist - TODO put this in prepare
    if not engine.dialect.has_table(engine, "ch_discovered"):
        DiscoveredCompany.__table__.create(engine)
        logging.info(f"Table {DiscoveredCompany.__table__} created")

    def save_not_found(not_found_list):
        s3.Object(*parse_s3_path(f"{s3_interim}/{datetime.datetime.now()}")).put(
            Body=json.dumps(not_found_list)
        )

    def consume(r):
        """ Enter row into database """
        logging.debug(r)
        if r.status_code == 200:
            engine.execute(
                "REPLACE INTO ch_discovered "
                "(company_number, response) "
                "VALUES (%s, %s)",
                (r.company_number, int(r.status_code)),
            )
        elif r.status_code == 404:
            logging.debug(f"404: {r.company_number} does not exist")
            not_found_list.append(r.company_number)
            if (len(not_found_list) % chunksize == 0 and len(not_found_list) > 0):
                save_not_found(not_found_list)
                not_found_list.clear()
        else:
            logging.debug(f"FAIL")

    # Make requests
    chunksize = 1000
    not_found_list = []
    dispatcher(candidates, api_key, consume=consume)
    save_not_found(not_found_list)  # Save remaining partial chunk

    logging.info(f"Marking task as done to {s3_path}")
    s3 = boto3.resource("s3")
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    if "BATCHPAR_outinfo" not in os.environ.keys():  # Local testing
        bucket = "nesta-production-intermediate"
        api_key = os.environ["CH_API_KEY"]
        input_info = f"s3://{bucket}/CH_batch_params_{api_key}_input"

        os.environ["BATCHPAR_CH_API_KEY"] = api_key
        os.environ["BATCHPAR_db_name"] = "dev"
        os.environ["BATCHPAR_inputinfo"] = input_info
        os.environ["BATCHPAR_outinfo"] = f"s3://{bucket}/CH_{api_key}_local_out"
        os.environ["BATCHPAR_done"] = "False"
        os.environ["BATCHPAR_test"] = "True"
        os.environ["BATCHPAR_config"] = os.environ["MYSQLDB"]

        os.environ["BATCHPAR_interiminfo"] = f"s3://{bucket}/CH_{api_key}_interim"

        # Input data
        s3 = boto3.resource("s3")
        s3_obj = s3.Object(*parse_s3_path(input_info))
        s3_obj.put(Body='["SC612773","SC612774","SC612775","SC612776","SO300397", "fail"]')

        level = logging.DEBUG
        level = logging.INFO
    else:
        level = logging.INFO

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(
        handlers=[log_stream_handler],
        level=level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )
    run()
