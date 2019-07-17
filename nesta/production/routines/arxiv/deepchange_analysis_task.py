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
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.routines.arxiv.arxiv_grid_task import GridTask


DEEPCHANGE_QUERY = misctools.find_filepath_from_pathstub('arxlive_deepchange.sql')
YEAR_THRESHOLD = 2012


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

        # collect articles, categories, institutes.
        # duplicate article ids exist due to authors from different institutes
        # contributing to a paper and multinational institutes.
        # There is one row per article / institiute / institute country
        with open(DEEPCHANGE_QUERY) as sql_query:
            df = pd.read_sql(sql_query.read(), self.engine)
        logging.info(f"Retrieved {len(df)} articles from database")

        # collect topics, determine which represents deep_learning and apply flag
        dl_topic_ids = deepchange_analysis.get_article_ids_by_term(self.engine,
                                                                   term='deep_learning',
                                                                   min_weight=0.2)
        df['is_dl'] = df.article_id.apply(lambda i: i in dl_topic_ids)
        logging.info(f"Flagged {df.is_dl.sum()} deep learning articles")

        df['date'] = df.apply(lambda row: row.article_updated or row.article_created,
                              axis=1)
        df['year'] = df.date.apply(lambda date: date.year)
        df = deepchange_analysis.add_before_date_flag(df,
                                                      date_column='date',
                                                      before_year=YEAR_THRESHOLD)

        # TODO: decide if we are applying de-duplication for each chart
        deduped = df.drop_duplicates('article_id')

        # first plot - dl/non dl distribution by country (top 20)
        pivot_by_country = (pd.pivot_table(deduped.groupby(['institute_country', 'is_dl'])
                                           .size()
                                           .reset_index(drop=False),
                            index='institute_country',
                            columns='is_dl',
                            values=0)
                            .apply(lambda x: 100 * (x / x.sum())))

        fig, ax = plt.subplots(figsize=(7, 4))
        (pivot_by_country.sort_values(True, ascending=False)[:20]
                         .sort_values(True)
                         .plot.barh(ax=ax))
        ax.set_xlabel('Percentage of DL papers in country')
        ax.set_ylabel('%')
        deepchange_analysis.plot_to_s3('arxlive-charts', 'figure_1.png', plt)

        # second plot - dl/non dl distribution by city (top 20)
        pivot_by_city = (pd.pivot_table(deduped.groupby(['institute_city', 'is_dl'])
                                        .size()
                                        .reset_index(drop=False),
                         index='institute_city',
                         columns='is_dl',
                         values=0)
                         .apply(lambda x: 100 * (x / x.sum())))

        fig, ax = plt.subplots(figsize=(7, 4))
        (pivot_by_city.sort_values(True, ascending=False)[:20]
                      .sort_values(True)
                      .plot.barh(ax=ax))
        ax.set_xlabel('Percentage of DL papers in city')
        ax.set_ylabel('%')
        deepchange_analysis.plot_to_s3('arxlive-charts', 'figure_2.png', plt)

        # third plot - percentage of dl papers by year
        # TODO: set reasonable limits on the y axis
        papers_by_year = pd.crosstab(df['year'], df['is_dl']).loc[2000:]
        papers_by_year = (100 * papers_by_year.apply(lambda x: x / x.sum(), axis=1))
        papers_by_year = papers_by_year.drop(False, axis=1)

        fig, ax = plt.subplots(figsize=(7, 4))
        papers_by_year.plot(legend=None)
        ax.set_xlabel('Percentage of DL papers by year')
        ax.set_ylabel('%')
        deepchange_analysis.plot_to_s3('arxlive-charts', 'figure_3.png', plt)

        '''
        charts:
        1. distribution of dl/non dl papers by country (horizontal bar)
        2. distribution of dl/non dl papers by city (horizontal bar)
        3. % ML papers by year (line)
        4. share of ML activity in arxiv subjects, pre/post 2012 (horizontal point / slope)
        5. rca, pre/post 2012 by country (horizontal point / slope)
        6. rca over time, citation > mean & top 50 countries (horizontal violin)

        table data:
        1. top countries by rca (moving window of last 12 months?)
        '''

        # mark as done
        logging.warning("Task complete")
        raise(NotImplementedError)  # stop the local scheduer from recording success while testing
        self.output().touch()
