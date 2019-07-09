"""
arXlive analysis for arXlive
============================

Luigi routine to extract arXive data and perform the analysis from the Deep learning,
deep change paper, placing the results in an S3 bucket to be picked up by the arXlive
front end.
"""
import logging
import luigi
import matplotlib.pyplot as plt
import pandas as pd

from nesta.packages.arxiv import deepchange_analysis
from nesta.production.luigihacks import misctools, mysqldb
from nesta.production.orms.orm_utils import get_mysql_engine, db_session
from nesta.production.routines.arxiv.arxiv_grid_task import GridTask


DEEPCHANGE_QUERY = misctools.find_filepath_from_pathstub('arxlive_deepchange.sql')


class AnalysisTask(luigi.Task):
    """Extract and analyse arXiv data to produce data and charts for the arXlive front
    end to consume.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        mag_config_path (str): Microsoft Academic Graph Api key configuration path
        insert_batch_size (int): number of records to insert into the database at once
                                 (not used in this task but passed down to others)
        articles_from_date (str): new and updated articles from this date will be
                                  retrieved. Must be in YYYY-MM-DD format
                                  (not used in this task but passed down to others)
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    mag_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    articles_from_date = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "ArxivAnalysis_{}".format(self.date)
        return mysqldb.MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        yield GridTask(date=self.date,
                       _routine_id=self._routine_id,
                       db_config_path=self.db_config_path,
                       db_config_env='MYSQLDB',
                       mag_config_path='mag.config',
                       test=self.test,
                       insert_batch_size=self.insert_batch_size,
                       articles_from_date=self.articles_from_date)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        # Base.metadata.create_all(self.engine)

        # with db_session(self.engine) as session:
        # query topics table and determine deep_learning topic id

        # collect articles, categories, institutes
        with open(DEEPCHANGE_QUERY) as sql_query:
            # TODO: consider adding param for threshold topic score
            df = pd.read_sql(sql_query.read(), self.engine, params={})

        # dummy value to be replaced with the topic modeling
        import numpy as np
        df['is_dl'] = np.random.choice([True, False], size=len(df), p=[0.7, 0.3])
        logging.info(df.is_dl.head())

        df['date'] = (df.article_created if df.article_updated.empty
                      else df.article_updated)
        df = deepchange_analysis.add_before_date_flag(df,
                                                      date_column='date',
                                                      before_year=2012)

        # first plot
        # TODO: decide if we are applying de-duplication
        deduped = df.drop_duplicates('article_id')

        pv_1 = (pd.pivot_table(deduped.groupby(['institute_country', 'is_dl'])
                               .size()
                               .reset_index(drop=False),
                index='institute_country',
                columns='is_dl',
                values=0)
                .apply(lambda x: 100 * (x / x.sum())))

        fig, ax = plt.subplots(figsize=(7, 4))
        pv_1.sort_values(True).plot.barh(ax=ax)
        ax.set_xlabel('Percentage of DL papers in country')
        ax.set_ylabel('')
        deepchange_analysis.plot_to_s3('arxlive-charts', 'figure_1.png', plt)

        '''
        charts:
        1. distribution of dl/non dl papers by country (horizontal bar)
        2. distribution of dl/non dl papers by city (horizontal bar)
        3. % ML papers by year (vertical bar)
        4. share of ML activity in arxiv subjects, pre/post 2012 (horizontal point)
        5. rca, pre/post 2012 by country (horizontal point)
        6. rca over time, citation > mean & top 50 countries (horizontal violin)

        table data:
        1. top countries by rca (moving window of last 12 months?)
        '''

        # mark as done
        logging.warning("Task complete")
        raise(NotImplementedError)  # stop the local scheduer from recording success
        self.output().touch()

