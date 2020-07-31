'''
Root task (arXlive)
===================

Luigi routine to collect all data from the arXiv api and load it to MySQL,
pipe to Elasticsearch, perform topic modelling, generate plots and
measure novelty.
'''

import luigi
import datetime
import logging
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.orms.arxiv_orm import Article
from nesta.core.orms.orm_utils import setup_es
from nesta.core.routines.arxiv.deepchange_analysis_task import AnalysisTask
from nesta.core.luigihacks.parameter import DictParameterPlus
from nesta.core.routines.arxiv.arxiv_lolvelty import ArxivElasticsearchTask

class RootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
        drop_and_recreate (bool): If in test mode, allows dropping the dev index from the ES database.
    
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter(default="mysqldb.config")
    production = luigi.BoolParameter(default=False)
    drop_and_recreate = luigi.BoolParameter(default=False)
    articles_from_date = luigi.Parameter(default=None)
    insert_batch_size = luigi.IntParameter(default=500)
    debug = luigi.BoolParameter(default=False)    

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)

        _routine_id = "{}-{}".format(self.date, self.production)
        grid_task_kwargs = {
            '_routine_id':_routine_id,
            'db_config_path':self.db_config_path,
            'db_config_env':'MYSQLDB',
            'mag_config_path':'mag.config',
            'test':not self.production,
            'insert_batch_size':self.insert_batch_size,
            'articles_from_date':self.articles_from_date,
            'date':self.date,
        }

        cherry_picked=(f'automl/{self.date}/COREX_TOPIC_MODEL'
                       '.n_hidden_27-0.VECTORIZER.binary_True'
                       f'.min_df_0-001.NGRAM.TEST_False.json')
        if not self.production:
            cherry_picked=(f'automl/{self.date}/COREX_TOPIC_MODEL'
                           '.n_hidden_33-0.VECTORIZER.binary_True'
                           '.min_df_0-001.NGRAM.TEST_True.json')

        kwargs = {'score_field': 'metric_novelty_article',
                  'fields': ['textBody_abstract_article']}
        test = not self.production
        routine_id = f"ArxivLolveltyTask-{self.date}-{test}"

        yield ArxivElasticsearchTask(date=self.date,
                                     process_batch_size=1000,
                                     routine_id=routine_id,
                                     grid_task_kwargs=grid_task_kwargs,
                                     test=not self.production,
                                     drop_and_recreate=self.drop_and_recreate,
                                     dataset='arxiv',
                                     endpoint='arxlive',
                                     entity_type='article',
                                     kwargs=kwargs,
                                     batchable=f3p("batchables/novelty"
                                                   "/lolvelty"),
                                     env_files=[f3p("nesta/"),
                                                f3p("config/mysqldb.config"),
                                                f3p("config/"
                                                    "elasticsearch.yaml")],
                                     job_def="py36_amzn1_image",
                                     job_name=routine_id,
                                     job_queue="HighPriority",
                                     region_name="eu-west-2",
                                     poll_time=10,
                                     memory=1024,
                                     max_live_jobs=10)

        yield AnalysisTask(date=self.date,
                           grid_task_kwargs=grid_task_kwargs,
                           _routine_id=_routine_id,
                           db_config_path=self.db_config_path,
                           db_config_env='MYSQLDB',
                           mag_config_path='mag.config',
                           test=not self.production,
                           insert_batch_size=self.insert_batch_size,
                           articles_from_date=self.articles_from_date,
                           cherry_picked=cherry_picked)


class EsOnlyRootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
        drop_and_recreate (bool): If in test mode, allows dropping the dev index from the ES database.
    
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter(default="mysqldb.config")
    production = luigi.BoolParameter(default=False)
    drop_and_recreate = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        routine_id = "ArxivESTask-{}-{}".format(self.date, self.production)
        logging.getLogger().setLevel(logging.INFO)
        yield Sql2EsTask(routine_id=routine_id,
                         date=self.date,
                         process_batch_size=10000,
                         drop_and_recreate=self.drop_and_recreate,
                         dataset='arxiv',
                         endpoint='arxlive',
                         id_field=Article.id,
                         entity_type='article',
                         db_config_env='MYSQLDB',
                         test=not self.production,
                         intermediate_bucket=('nesta-production'
                                              '-intermediate'),
                         batchable=f3p('batchables/arxiv/'
                                       'arxiv_elasticsearch'),
                         env_files=[f3p('nesta/'),
                                    f3p('config/'
                                        'mysqldb.config'),
                                    f3p('datasets/arxiv.json'),
                                    f3p('config/'
                                        'elasticsearch.config')],
                         job_def='py36_amzn1_image',
                         job_name=routine_id,
                         job_queue='HighPriority',
                         region_name='eu-west-2',
                         memory=2048,
                         poll_time=10,
                         max_live_jobs=100)
