import luigi
import os

from sqlalchemy import or_
from nesta.production.luigihacks import s3
from nesta.production.orms.arxiv_orm import article_categories as ArtCat
from nesta.production.orms.arxiv_orm import Article
#from nesta.production.routines.automl import AutoMLTask
from nesta.production.orms.orm_utils import get_mysql_engine, db_session
import json


THIS_PATH = os.path.dirname(os.path.realpath(__file__))
CHAIN_PARAMETER_PATH = os.path.join(THIS_PATH,
                                    "topic_process_task_chain.json")


class PrepareArxivS3Data(luigi.Task):
    """Task that pipes SQL text fields to a number of S3 JSON files.
    This is particularly useful for preparing autoML tasks.
    """
    s3_path_out = luigi.Parameter("s3://nesta-arxlive/raw-inputs/data")
    db_conf_env = luigi.Parameter(default="MYSQLDB")
    chunksize = luigi.IntParameter(default=10000)
    test = luigi.BoolParameter(default=True)

    def output(self):
        return s3.S3Target(f"{self.s3_path_out}.{self.test}.length")

    def write_to_s3(self, data, ichunk):
        f = s3.S3Target(f"{self.s3_path_out}-"
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

    def requires(self):
        # Launch the dependencies
        yield PrepareArxivS3Data(
        # yield AutoMLTask(s3_path_in="LOCATION OF FLAT FILES TO BE BATCH GRAMMED",
        #                  s3_path_prefix="s3://nesta-automl/arxiv/",
        #                  task_chain_filepath=CHAIN_PARAMETER_PATH,
        #                  test=not self.production)
