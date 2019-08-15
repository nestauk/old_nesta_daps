import logging
import os
from ast import literal_eval

import boto3

from nesta.packages.companies_house.find_dissolved import dispatcher
from nesta.production.luigihacks.s3 import parse_s3_path

API_CALLS, API_TIME = 3, 3


def run():
    """ """
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]

    s3_input = os.environ["BATCHPAR_inputinfo"]
    api_key = os.environ["BATCHPAR_CH_API_KEY"]

    # Read candidates from S3 input
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_input))
    candidates = json.loads(s3_obj.get()['Body'].read())

    if test:
        candidates = candidates[:10]

    def consume(row):
        """ Enter row into database """
        return print

    # Make requests
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dispatcher(candidates, api_key, consume=consume))

    logging.info(f"Marking task as done to {s3_path}")
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
