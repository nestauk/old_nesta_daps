"""
Clustering task
====================
Cluster vectors stored in S3.
"""

from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.gtr_orm import Base, DocumentClusters
from nesta.core.orms.orm_utils import get_mysql_engine, try_until_allowed
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config
from nesta.core.routines.embed_topics.doc_vectors_batch import TextVectors
from nesta.packages.embed_topics.embed_clustering import clustering
from nesta.packages.misc_utils.np_utils import arr2dic

import numpy as np
import json
import boto3
import luigi
import datetime
import os
import logging

# params to set
my_bucket = None  # clio-text2vec, bucket with {ID:vector} dicts
output_bucket = None  # clio-vectors2clusters, bucket to store clustering model


class ClusterVectors(luigi.Task):
    """A task...
    Normally put this in a separate file and import it.
    Args:
        date (datetime): Date used to label the completed task
        test (bool): If True pipeline is running in test mode
    """
    date = luigi.DateParameter(default=datetime.datetime.today())
    test = luigi.BoolParameter()

    def requires(self):
        yield TextVectors(  # just pass on parameters the previous tasks need:
                           _routine_id=self._routine_id,
                           test=self.test,
                           insert_batch_size=self.insert_batch_size,
                           db_config_env='MYSQLDB')

    def output(self):
        """Points to the output database engine where the task is marked as done."""
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "vec2cluster <dummy>"  # Note, not a real table
        update_id = "ClusterVectors_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        try_until_allowed(Base.metadata.create_all, self.engine)
        # Get data from S3 and merge {IDs:vectors} dictionaries.
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('clio-text2vec')

        dicts = {}
        for stored_obj in my_bucket.objects.all():
            obj = s3.Object('clio-text2vec', stored_obj.key)
            d = json.loads(obj.get()['Body']._raw_stream.read())
            dicts.update(d)

        # Fit cluster model
        logging.info("Clustering vectors")
        vectors = np.array(list(dicts.values()))
        gmm = clustering(vectors)
        # Assign fuzzy clusters to documents.
        clusters_probs = gmm.predict_proba(np.array(vectors))
        # Remove fuzzy clusters with less than .1 probability.
        topics_arr = [arr2dic(probs, thresh=.1) for probs in clusters_probs]

        # Save cluster model on S3
        s3.Object('clio-vectors2clusters', 'gmm.pickle').put(Body=gmm)
        logging.info("Saved fitted GMM.")

        # Save {IDs:topics} on MYSQL
        # Map IDs to topics.
        d = {id_: arr for id_, arr in zip(dicts.keys(), topics_arr)}
        insert_data(self.db_config_env, 'mysqldb', database, Base, DocumentClusters, )
        # Mark as done
        logging.info("Task complete")

        # if running locally, consider using:
        # raise NotImplementedError
        # while testing to prevent the local scheduler from marking the task as done
        self.output().touch()
