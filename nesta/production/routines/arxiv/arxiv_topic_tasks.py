import luigi
import os
import datetime
import json
import logging

from sqlalchemy import or_
from nesta.production.luigihacks import s3
from nesta.production.orms.arxiv_orm import article_categories as ArtCat
from nesta.production.orms.arxiv_orm import Article
from nesta.production.luigihacks.automl import AutoMLTask
from nesta.production.orms.orm_utils import get_mysql_engine, db_session


THIS_PATH = os.path.dirname(os.path.realpath(__file__))
CHAIN_PARAMETER_PATH = os.path.join(THIS_PATH,
                                    "topic_process_task_chain.json")


class PrepareArxivS3Data(luigi.Task):
    """Task that pipes SQL text fields to a number of S3 JSON files.
    This is particularly useful for preparing autoML tasks.
    """
    s3_path_out = luigi.Parameter()
    db_conf_env = luigi.Parameter(default="MYSQLDB")
    chunksize = luigi.IntParameter(default=10000)
    test = luigi.BoolParameter(default=True)

    def output(self):
        return s3.S3Target(f"{self.s3_path_out}/"
                           f"data.{self.test}.length")

    def write_to_s3(self, data, ichunk):
        f = s3.S3Target(f"{self.s3_path_out}/data."
                        f"{ichunk}-{self.test}.json").open("wb")
        f.write(json.dumps(data).encode('utf-8'))
        f.close()
        return [], ichunk+1

    def run(self):
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_conf_env, 'mysqldb', database)
        with db_session(engine) as session:
            # Make the query
            result = (session
                      .query(Article.id, Article.abstract)
                      .join(ArtCat)
                      .filter(or_(ArtCat.c.category_id.like("cs.%"),
                                  ArtCat.c.category_id == "stat.ML")))
            # Buffer the data in
            data, ichunk = [], 0
            for i, (uid, abstract) in enumerate(result.yield_per(self.chunksize)):
                data.append({'id': uid, 'body': abstract})
                if len(data) == self.chunksize:
                    data, ichunk = self.write_to_s3(data, ichunk)
        # Final flush
        if len(data) > 0:
            self.write_to_s3(data, ichunk)
        # Write the output length as well, for book-keeping
        f = self.output().open("wb")
        f.write(str(i).encode("utf-8"))
        f.close()


class TopicRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    s3_path_prefix = luigi.Parameter(default="s3://nesta-arxlive")
    raw_data_path = luigi.Parameter(default="raw-inputs")
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        logging.getLogger().setLevel(logging.INFO)

        # Launch the dependencies
        raw_data_path = (f"{self.s3_path_prefix}/"
                         f"{self.raw_data_path}/{self.date}")
        test = not self.production
        yield PrepareArxivS3Data(s3_path_out=raw_data_path,
                                 test=test)
        yield AutoMLTask(s3_path_in=f"{raw_data_path}/data.{test}.length",
                         s3_path_prefix=f"{self.s3_path_prefix}/automl/",
                         task_chain_filepath=CHAIN_PARAMETER_PATH,
                         test=test)
