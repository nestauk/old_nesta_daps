"""
Clustering task
====================
Cluster vectors stored in S3.
"""

from nesta.core.orms.gtr_orm import Base, DocumentClusters
from nesta.core.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config
from nesta.core.routines.embed_topics.doc_vectors_batch import TextVectors
from nesta.packages.nlp_utils.embed_clustering import clustering
from nesta.packages.misc_utils.np_utils import arr2dic
from nesta.packages.format_utils.listtools import flatten_lists, dicts2sql_format

import pickle
import numpy as np
import json
import boto3
import luigi
import datetime
import os
import logging


class ClusterVectors(luigi.Task):
    """A task...
    Normally put this in a separate file and import it.
    Args:
        date (datetime): Date used to label the completed task
        test (bool): If True pipeline is running in test mode
    """

    date = luigi.DateParameter(default=datetime.datetime.today())
    test = luigi.BoolParameter()
    db_config_env = luigi.Parameter()

    # def requires(self):
    #     yield TextVectors()  # add params

    def output(self):
        """Points to the output database engine where the task is marked as done."""
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = "dev" if self.test else "production"
        db_config["table"] = "vec2cluster <dummy>"  # Note, not a real table
        update_id = "ClusterVectors_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = "dev" if self.test else "production"
        self.engine = get_mysql_engine(self.db_config_env, "mysqldb", database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # Get data from S3 and merge {IDs:vectors} dictionaries.
        s3 = boto3.resource("s3")
        input_bucket = s3.Bucket("clio-text2vec")
        output_bucket = "clio-vectors2clusters"

        dicts = {}
        for stored_obj in input_bucket.objects.all():
            obj = s3.Object("clio-text2vec", stored_obj.key)
            d = json.loads(obj.get()["Body"]._raw_stream.read())
            dicts.update(d)

        logging.info(f"Loaded {len(dicts)} objects from {input_bucket}")

        # Fit cluster model
        logging.info("Clustering vectors")
        vectors = np.array(list(dicts.values()))
        gmm = clustering(vectors)
        # Assign fuzzy clusters to documents.
        clusters_probs = gmm.predict_proba(np.array(vectors))
        # Remove fuzzy clusters with less than .1 probability.
        topics_arr = [arr2dic(probs, thresh=0.1) for probs in clusters_probs]

        # Save cluster model on S3
        s3.Object(output_bucket, "gmm.pickle").put(Body=pickle.dumps(gmm))
        logging.info("Saved fitted GMM.")

        # Save {IDs:topics} on MYSQL
        # Map IDs to topics.
        # d = {id_: arr for id_, arr in zip(dicts.keys(), topics_arr)}
        items = flatten_lists(dicts2sql_format(dicts, topics_arr))
        insert_data(
            self.db_config_env, "mysqldb", database, Base, DocumentClusters, items
        )
        # Mark as done
        logging.info("Task complete")

        # if running locally, consider using:
        # raise NotImplementedError
        # while testing to prevent the local scheduler from marking the task as done
        self.output().touch()


class RootTask(luigi.WrapperTask):
    """Collect the supplied parameters and call the previous task.
    Args:
        date (datetime): Date used to label the completed task
        production (bool): enable test (False) or production mode (True)
    """

    date = luigi.DateParameter(default=datetime.datetime.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        """Call the previous task in the pipeline."""

        logging.getLogger().setLevel(logging.INFO)
        return ClusterVectors(
            date=self.date, test=not self.production, db_config_env="MYSQLDB"
        )
# job_def="py36_amzn1_image",
#                                    job_name="cluster-vectors-%s" % self.date,
#                                    job_queue="HighPriority",
#                                    region_name="eu-west-2",
#                                    env_files=[f3p("nesta/nesta/"),
#                                               f3p("config/mysqldb.config")])
