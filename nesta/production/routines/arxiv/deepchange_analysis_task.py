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
import numpy as np
import pandas as pd

from nesta.packages.arxiv import deepchange_analysis as dc
from nesta.production.luigihacks import misctools, mysqldb
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.routines.arxiv.arxiv_topic_tasks import WriteTopicTask
from nesta.production.luigihacks.parameter import DictParameterPlus

DEEPCHANGE_QUERY = misctools.find_filepath_from_pathstub('arxlive_deepchange.sql')
YEAR_THRESHOLD = 2012
MIN_RCA_YEAR = 2007  # minimum year when calculating rca pre 2012
N_TOP = 20  # number of countries / cities / categories to show
COLOR_A = '#992b15'
COLOR_B = '#d18270'
STATIC_FILES_BUCKET = 'arxlive-static-files'


class AnalysisTask(luigi.Task):
    """Extract and analyse arXiv data to produce data and charts for the arXlive front
    end to consume.

    Proposed charts:
        1. distribution of dl/non dl papers by country (horizontal bar)
        2. distribution of dl/non dl papers by city (horizontal bar)
        3. % ML papers by year (line)
        4. share of ML activity in arxiv subjects, pre/post 2012 (horizontal point / slope)
        5. rca, pre/post 2012 by country (horizontal point / slope)
        6. rca over time, citation > mean & top 50 countries (horizontal violin) [NOT DONE]

    Proposed table data:
        1. top countries by rca (moving window of last 12 months?) [NOT DONE]

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
    s3_path_prefix = luigi.Parameter(default="s3://nesta-arxlive")
    raw_data_path = luigi.Parameter(default="raw-inputs")
    grid_task_kwargs = DictParameterPlus()
    cherry_picked = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "ArxivAnalysis_{}".format(self.date)
        return mysqldb.MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        s3_path_prefix=(f"{self.s3_path_prefix}/"
                        f"automl/{self.date}")
        data_path = (f"{self.s3_path_prefix}/"
                     f"{self.raw_data_path}/{self.date}")
        yield WriteTopicTask(raw_s3_path_prefix=self.s3_path_prefix,
                             s3_path_prefix=s3_path_prefix,
                             data_path=data_path,
                             date=self.date,
                             cherry_picked=self.cherry_picked,
                             test=self.test,
                             grid_task_kwargs=self.grid_task_kwargs)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # collect articles, categories, institutes.
        # duplicate article ids exist due to authors from different institutes
        # contributing to a paper and multinational institutes.
        # There is one row per article / institiute / institute country
        with open(DEEPCHANGE_QUERY) as sql_query:
            df = pd.read_sql(sql_query.read(), self.engine)
        logging.info(f"Dataset contains {len(df)} articles")

        # collect topics, determine which represents deep_learning and apply flag
        dl_topic_ids = dc.get_article_ids_by_term(self.engine,
                                                  term='deep_learning',
                                                  min_weight=0.2)
        df['is_dl'] = df.article_id.apply(lambda i: i in dl_topic_ids)
        logging.info(f"Flagged {df.is_dl.sum()} deep learning articles in dataset")

        df['date'] = df.apply(lambda row: row.article_updated or row.article_created,
                              axis=1)
        df['year'] = df.date.apply(lambda date: date.year)
        df = dc.add_before_date_flag(df,
                                     date_column='date',
                                     before_year=YEAR_THRESHOLD)

        # first plot - dl/non dl distribution by country (top n)
        pivot_by_country = (pd.pivot_table(df.groupby(['institute_country', 'is_dl'])
                                           .size()
                                           .reset_index(drop=False),
                            index='institute_country',
                            columns='is_dl',
                            values=0)
                            .apply(lambda x: 100 * (x / x.sum()))
                            .rename(columns={True: 'DL', False: 'non DL'}))

        fig, ax = plt.subplots(figsize=(7, 4))
        (pivot_by_country.sort_values('DL', ascending=False)[:N_TOP]
                         .sort_values('DL')
                         .plot.barh(ax=ax, color=[COLOR_B, COLOR_A], width=0.8))
        ax.set_xlabel('Percentage of DL papers in country')
        ax.set_ylabel('')
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(labels=[labels[1], labels[0]],
                  handles=[handles[1], handles[0]],
                  title='')
        dc.plot_to_s3(STATIC_FILES_BUCKET, 'static/figure_1.png', plt)

        # second plot - dl/non dl distribution by city (top n)
        pivot_by_city = (pd.pivot_table(df.groupby(['institute_city', 'is_dl'])
                                        .size()
                                        .reset_index(drop=False),
                         index='institute_city',
                         columns='is_dl',
                         values=0)
                         .apply(lambda x: 100 * (x / x.sum()))
                         .rename(columns={True: 'DL', False: 'non DL'}))

        fig, ax = plt.subplots(figsize=(7, 4))
        (pivot_by_city.sort_values('DL', ascending=False)[:N_TOP]
                      .sort_values('DL')
                      .plot.barh(ax=ax, color=[COLOR_B, COLOR_A], width=0.8))
        ax.set_xlabel('Percentage of DL papers in city')
        ax.set_ylabel('')
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(labels=[labels[1], labels[0]],
                  handles=[handles[1], handles[0]],
                  title='')
        dc.plot_to_s3(STATIC_FILES_BUCKET, 'static/figure_2.png', plt)

        # third plot - percentage of dl papers by year
        deduped = df.drop_duplicates('article_id')

        start_year = 2000
        papers_by_year = pd.crosstab(deduped['year'], deduped['is_dl']).loc[start_year:]
        papers_by_year = (100 * papers_by_year.apply(lambda x: x / x.sum(), axis=1))
        papers_by_year = papers_by_year.drop(False, axis=1)  # drop non-dl column

        fig, ax = plt.subplots(figsize=(7, 4))
        papers_by_year.plot(ax=ax, legend=None, color=COLOR_A)
        plt.xlabel('Year')
        plt.xticks(np.arange(min(papers_by_year.index), max(papers_by_year.index) + 1, 1))
        ax.set_xticklabels(['' if i % 2 else y
                           for i, y in enumerate(papers_by_year.index)])

        plt.ylabel('Percentage of papers')
        dc.plot_to_s3(STATIC_FILES_BUCKET, 'static/figure_3.png', plt, pad_x=True)

        # fourth plot - share of DL activity by arxiv subject pre/post threshold
        all_categories = list({cat for cats in df.arxiv_category_descs
                               for cat in cats.split('|')})

        cat_period_container = []

        for cat in all_categories:
            subset = df.loc[[cat in x for x in df['arxiv_category_descs']], :]
            subset_ct = pd.crosstab(subset[f'before_{YEAR_THRESHOLD}'],
                                    subset.is_dl,
                                    normalize=0)
            subset_ct.index = [f'After {YEAR_THRESHOLD}', f'Before {YEAR_THRESHOLD}']

            # this try /except may not be required when running on the full dataset
            try:
                cat_period_container.append(pd.Series(subset_ct[True], name=cat))
            except KeyError:
                pass

        cat_thres_df = (pd.concat(cat_period_container, axis=1)
                        .T
                        .sort_values(f'After {YEAR_THRESHOLD}', ascending=False))
        other = cat_thres_df[N_TOP:].mean().rename('Other')
        cat_thres_df = cat_thres_df[:N_TOP].append(other)

        fig, ax = plt.subplots(figsize=(7, 4))
        (100 * cat_thres_df[f'Before {YEAR_THRESHOLD}']).plot(markeredgecolor=COLOR_B,
                                                              marker='o',
                                                              color=COLOR_B,
                                                              ax=ax,
                                                              markerfacecolor=COLOR_B)
        (100 * cat_thres_df[f'After {YEAR_THRESHOLD}']).plot(markeredgecolor=COLOR_A,
                                                             marker='o',
                                                             color=COLOR_A,
                                                             ax=ax,
                                                             markerfacecolor=COLOR_A)
        ax.vlines(np.arange(len(cat_thres_df)),
                  ymin=len(cat_thres_df) * [0],
                  ymax=100 * cat_thres_df[f'After {YEAR_THRESHOLD}'],
                  linestyle=':')

        ax.set_xticks(np.arange(len(cat_thres_df)))
        ax.set_xticklabels(cat_thres_df.index, rotation=40, ha='right')
        ax.set_xlabel('arXiv category')

        ax.set_ylabel('DL as % of all papers \n in each category')
        ax.set_ylabel('Percentage of DL papers')
        ax.legend()
        dc.plot_to_s3(STATIC_FILES_BUCKET, 'static/figure_4.png', plt, pad_x=True)

        # fifth chart - changes in specialisation before / after threshold (top n countries)

        # flag papers with citations above the yearly median
        avg_citation_counts = df[['year', 'citation_count']].groupby('year').quantile(.5)
        df['highly_cited'] = df.apply(dc.highly_cited, args=(avg_citation_counts,),
                                      axis=1)

        # apply filters before calculating rca
        # TODO: remove the bottom 10% of countries here
        highly_cited = df[(df.highly_cited) & (df.year >= MIN_RCA_YEAR)]

        # calculate revealed comparative advantage
        pre_threshold_rca = dc.calculate_rca_by_country(
            highly_cited[highly_cited[f'before_{YEAR_THRESHOLD}']],
            country_column='institute_country',
            commodity_column='is_dl')

        post_threshold_rca = dc.calculate_rca_by_country(
            highly_cited[~highly_cited[f'before_{YEAR_THRESHOLD}']],
            country_column='institute_country',
            commodity_column='is_dl')

        rca_combined = (pd.merge(pre_threshold_rca, post_threshold_rca,
                                 left_index=True, right_index=True,
                                 suffixes=('_before', '_after'))
                        .rename(columns={'is_dl_before': f'Before {YEAR_THRESHOLD}',
                                         'is_dl_after': f'After {YEAR_THRESHOLD}'})
                        .sort_values(f'After {YEAR_THRESHOLD}', ascending=False))

        # limit to top countries by dl activity
        top_dl_countries = (df[['institute_country', 'is_dl']]
                            .groupby('institute_country')
                            .sum()
                            .sort_values('is_dl', ascending=False)[:N_TOP]
                            .index
                            .to_list())
        rca_combined_top = rca_combined[rca_combined.index.isin(top_dl_countries)]

        fig, ax = plt.subplots(figsize=(7, 4))
        rca_combined_top[f'Before {YEAR_THRESHOLD}'].plot(markeredgecolor=COLOR_B,
                                                          marker='o',
                                                          color='white',
                                                          ax=ax,
                                                          markerfacecolor=COLOR_B)
        rca_combined_top[f'After {YEAR_THRESHOLD}'].plot(markeredgecolor=COLOR_A,
                                                         marker='o',
                                                         color='white',
                                                         ax=ax,
                                                         markerfacecolor=COLOR_A)
        col = [COLOR_A if x > y else '#d18270'
               for x, y in zip(rca_combined_top[f'After {YEAR_THRESHOLD}'],
                               rca_combined_top[f'Before {YEAR_THRESHOLD}'])]
        ax.vlines(np.arange(len(rca_combined_top)),
                  ymin=rca_combined_top[f'Before {YEAR_THRESHOLD}'],
                  ymax=rca_combined_top[f'After {YEAR_THRESHOLD}'],
                  linestyle=':',
                  color=col)
        ax.hlines(y=1,
                  xmin=0,
                  xmax=len(rca_combined_top),
                  color='darkgrey',
                  linestyle='--')
        ax.set_xticks(np.arange(len(rca_combined_top)))
        ax.set_xticklabels(rca_combined_top.index, rotation=40, ha='right')
        ax.legend()
        ax.set_ylabel('RCA')
        ax.set_xlabel('')

        dc.plot_to_s3(STATIC_FILES_BUCKET, 'static/figure_5.png', plt, pad_x=True)

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
