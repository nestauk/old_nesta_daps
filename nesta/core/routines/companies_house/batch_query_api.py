import datetime
import json
import logging
import os
from itertools import chain

import boto3
import luigi
import numpy as np
from pandas import read_sql_query

from nesta.core.luigihacks import autobatch, s3
from nesta.core.luigihacks.misctools import (find_filepath_from_pathstub,
                                             get_config)
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.orms.orm_utils import (db_session, get_mysql_engine,
                                       try_until_allowed)
from nesta.packages.companies_house.find_dissolved import \
    generate_company_number_candidates

S3 = boto3.resource("s3")
S3_PREFIX = "s3://nesta-production-intermediate/CH_batch_params"
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

MYSQLDB_ENV = "MYSQLDB"


class CHBatchQuery(autobatch.AutoBatchTask):
    """
    Args:
        date (datetime): Date used to label the outputs
        batchable (str): Path to the directory containing the run.py batchable
        job_def (str): Name of the AWS job definition
        job_name (str): Name given to this AWS batch job
        job_queue (str): AWS batch queue
        region_name (str): AWS region from which to batch
        poll_time (int): Time between querying the AWS batch job status
    """

    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        pass

    def output(self):
        """ """
        db_config = get_config(os.environ[MYSQLDB_ENV], "mysqldb")
        db_config["database"] = "dev" if self.test else "production"
        db_config["table"] = "CompaniesHouse <dummy>"
        update_id = f"CHQueryAPI_{self.date}"
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        """Prepare the batch job parameters"""
        db_name = "dev" if self.test else "production"

        with open(os.environ["CH_API"], "r") as f:
            api_key_l = f.read().split(",")

        def already_found():
            """ From SQL"""
            engine = get_mysql_engine(MYSQLDB_ENV, "mysqldb", db_name)
            return set(
                read_sql_query(
                    "SELECT company_number from ch_companies", engine
                ).company_number.to_list()
            )

        def already_not_found():
            """ From s3 `interiminfo`"""

            # Get filenames
            files = list(
                get_matching_s3_keys(
                    "nesta-production-intermediate", prefix=f"CH_{key}_interim"
                )
            )
            # Concatenate files into set
            return set(
                chain(
                    *(
                        json.loads(
                            s3.Object("nesta-production-intermediate", f)
                            .get()["Body"]
                            .read()
                        )
                        for f in files
                    )
                )
            )

        prefixes = ["0", "OC", "LP", "SC", "SO", "SL", "NI", "R", "NC", "NL"]
        candidates = list(
            map(
                list,
                np.array_split(
                    list(set(
                        chain(
                            *[
                                generate_company_number_candidates(prefix)
                                for prefix in prefixes
                            ]
                        )
                    )
                    - already_found()
                    - already_not_found()),
                    len(api_key_l),
                ),
            )
        )
        logging.info(f"candidates: {list(map(len, candidates))}")

        if self.test:
            candidates = candidates[:2]
            api_key_l = api_key_l[:2]

        job_params = []
        for api_key, batch_candidates in zip(api_key_l, candidates):
            key = api_key
            # Save candidate numbers to S3 by api_key
            inputinfo = f"{S3_PREFIX}_{key}_input"
            (
                S3.Object(*s3.parse_s3_path(inputinfo)).put(
                    Body=json.dumps(batch_candidates)
                )
            )
            interiminfo = f"s3://nesta-production-intermediate/CH_{key}_interim"

            params = {
                "CH_API_KEY": api_key,
                "db_name": "CompaniesHouse <dummy>",
                "inputinfo": inputinfo,
                "interiminfo": interiminfo,
                "outinfo": f"{S3_PREFIX}_{key}",
                "test": self.test,
                "done": key in DONE_KEYS,
            }

            logging.info(params)
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        """ Touch the checkpoint """
        self.output().touch()


class RootTask(luigi.Task):
    """
    Args:
        date (`datetime`): Date used to label the outputs
    """

    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        """Get the output from the batchtask"""
        return CHBatchQuery(
            date=self.date,
            batchable=("~/nesta/nesta/production/" "batchables/companies_house/"),
            job_def="standard_image",
            job_name="ch-batch-api-%s" % self.date,
            env_files=[find_filepath_from_pathstub("nesta/nesta")],
            job_queue="MinimalCpus",
            region_name="eu-west-2",
            poll_time=5,
        )

    def output(self):
        pass

    def run(self):
        pass


# TODO: factor out into some utils file
def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix,)
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj["Key"]
