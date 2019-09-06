"""
arXlive analysis for arXlive
============================

Luigi routine to extract arXive data and perform the 
analysis from the Deep learning,
deep change paper, placing the results in an 
S3 bucket to be picked up by the arXlive front end.
"""
import logging
import luigi
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import pandas as pd
from datetime import datetime as dt

from nesta.packages.arxiv import deepchange_analysis as dc
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks import mysqldb
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.routines.arxiv.arxiv_topic_tasks import WriteTopicTask
from nesta.core.luigihacks.parameter import DictParameterPlus

matplotlib.rcParams['figure.figsize'] = (20, 13)
matplotlib.rcParams.update({'font.size': 25, "axes.labelpad":10})

ORDERED_QUERIES = [f3p(x) for x in 
                   ('arxlive1_filter_cats.sql', 
                    'arxlive2_join_insts.sql', 
                    'arxlive3_group_cats.sql', 
                    'arxlive4_read_final.sql')]
YEAR_THRESHOLD = 2012
MIN_RCA_YEAR = 2007  # minimum year when calculating rca pre 2012
N_TOP = 15  # number of countries / cities / categories to show
COLOR_A = '#631607'
COLOR_B = '#d68b7a'
STATIC_FILES_BUCKET = 'arxlive-static-files'

def sql_queries():
    for i, filepath in enumerate(ORDERED_QUERIES):
        is_last = bool(i+1 == len(ORDERED_QUERIES))
        with open(filepath) as f:
            query = f.read()
            yield query, is_last


class AnalysisTask(luigi.Task):
    """Extract and analyse arXiv data to produce data 
    and charts for the arXlive front end to consume.

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
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXlive <dummy>"  # NB: not a real table
        update_id = "ArxivAnalysis_{}_{}".format(self.date, self.test)
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
        # Threshold for testing
        year_threshold = 2008 if self.test else YEAR_THRESHOLD
        test_label = 'test' if self.test else ''

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env,
                                       'mysqldb', database)

        # All queries except last prepare temporary tables
        # and the final query produces the dataframe
        # which collects data, such that there is one row per
        # article / institute / institute country
        for query, is_last in sql_queries():
            if not is_last:
                self.engine.execute(query)
        df = pd.read_sql(query, self.engine)
        logging.info(f"Dataset contains {len(df)} articles")

        # Manual hack to factor Hong Kong outside of China
        for city in ["Hong Kong", "Tsuen Wan", 
                     "Tuen Mun", "Tai Po", "Sai Kung"]:
            df.loc[df.institute_city == f"{city}, CN", 
                   "institute_country"] = "Hong Kong"

        # Manual hack to factor out transnational corps
        countries = set(df.institute_country)
        df['is_multinational'] = df['institute_name'].apply(lambda x: dc.is_multinational(x, countries))
        df.loc[df.is_multinational, 'institute_city'] = df.loc[df.is_multinational, 'institute_name'].apply(lambda x: ''.join(x.split("(")[:-1]))
        df.loc[df.is_multinational, 'institute_country'] = "Transnationals"

        # collect topics, determine which represents
        # deep_learning and apply flag
        terms = ["deep", "deep_learning", "reinforcement",
                 "neural_networks", "neural_network"]
        min_weight = 0.1 if self.test else 0.3
        dl_topic_ids = dc.get_article_ids_by_terms(self.engine,
                                                   terms=terms,
                                                   min_weight=min_weight)

        df['is_dl'] = df.article_id.apply(lambda i: i in dl_topic_ids)
        logging.info(f"Flagged {df.is_dl.sum()} deep learning articles in dataset")

        df['date'] = df.apply(lambda row: row.article_updated or row.article_created,
                              axis=1)
        df['year'] = df.date.apply(lambda date: date.year)
        df = dc.add_before_date_flag(df,
                                     date_column='date',
                                     before_year=year_threshold)

        # first plot - dl/non dl distribution by country (top n)
        pivot_by_country = (pd.pivot_table(df.groupby(['institute_country', 'is_dl'])
                                           .size()
                                           .reset_index(drop=False),
                                           index='institute_country',
                                           columns='is_dl',
                                           values=0)
                            .apply(lambda x: 100 * (x / x.sum()))
                            .rename(columns={True: 'DL', False: 'non DL'}))
        fig, ax = plt.subplots()
        (pivot_by_country.sort_values('DL', ascending=False)[:N_TOP]
         .sort_values('DL')
         .plot.barh(ax=ax, color=[COLOR_B, COLOR_A], width=0.6))
        ax.set_xlabel('Percentage of DL papers in arXiv CompSci\ncategories, by country')
        ax.set_ylabel('')
        handles, labels = ax.get_legend_handles_labels()
        _ = ax.legend(labels=[labels[1], labels[0]],
                      handles=[handles[1], handles[0]],
                      title='')
        dc.plot_to_s3(STATIC_FILES_BUCKET, f'static/figure_1{test_label}.png', plt)

        # second plot - dl/non dl distribution by city (top n)
        pivot_by_city = (pd.pivot_table(df.groupby(['institute_city', 'is_dl'])
                                        .size()
                                        .reset_index(drop=False),
                                        index='institute_city',
                                        columns='is_dl',
                                        values=0)
                         .apply(lambda x: 100 * (x / x.sum()))
                         .rename(columns={True: 'DL', False: 'non DL'}))
        fig, ax = plt.subplots()
        (pivot_by_city.sort_values('DL', ascending=False)[:N_TOP]
         .sort_values('DL')
         .plot.barh(ax=ax, color=[COLOR_B, COLOR_A], width=0.8))
        ax.set_xlabel('Percentage of DL papers in arXiv CompSci\ncategories, by city or multinational')
        ax.set_ylabel('')
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(labels=[labels[1], labels[0]],
                  handles=[handles[1], handles[0]],
                  title='')
        dc.plot_to_s3(STATIC_FILES_BUCKET, f'static/figure_2{test_label}.png', plt)

        # third plot - percentage of dl papers by year
        deduped = df.drop_duplicates('article_id')
        start_year = 2000
        papers_by_year = pd.crosstab(deduped['year'], deduped['is_dl']).loc[start_year:]
        papers_by_year = (100 * papers_by_year.apply(lambda x: x / x.sum(), axis=1))
        papers_by_year = papers_by_year.drop(False, axis=1)  # drop non-dl column
        fig, ax = plt.subplots(figsize=(20,8))
        papers_by_year.plot(ax=ax, legend=None, color=COLOR_A, linewidth=10)
        plt.xlabel('\nYear of paper publication')
        plt.ylabel('Percentage of DL papers\nin arXiv CompSci categories\n')
        plt.xticks(np.arange(min(papers_by_year.index), max(papers_by_year.index) + 1, 1))
        ax.set_xticklabels(['' if i % 2 else y
                            for i, y in enumerate(papers_by_year.index)])
        dc.plot_to_s3(STATIC_FILES_BUCKET, f'static/figure_3{test_label}.png', plt)

        # fourth plot - share of DL activity by arxiv
        # subject pre/post threshold
        df_all_cats = pd.read_sql("SELECT * FROM arxiv_categories",
                                  self.engine)
        condition = (df_all_cats.id.str.startswith('cs.') |
                     (df_all_cats.id.str == 'stat.ML'))
        all_categories = list(df_all_cats.loc[condition].description)
        _before = f'Before {year_threshold}'
        _after = f'After {year_threshold}'

        cat_period_container = []
        for cat in all_categories:
            subset = df.loc[[cat in x
                             for x in df['arxiv_category_descs']], :]
            subset_ct = pd.crosstab(subset[f'before_{year_threshold}'],
                                    subset.is_dl,
                                    normalize=0)
            # This is true for some categories in dev mode
            # due to a smaller dataset
            if list(subset_ct.index) != [False, True]:
                continue
            subset_ct.index = [_after, _before]
            # this try /except may not be required when
            # running on the full dataset
            try:
                cat_period_container.append(pd.Series(subset_ct[True],
                                                      name=cat))
            except KeyError:
                pass

        cat_thres_df = (pd.concat(cat_period_container, axis=1)
                        .T
                        .sort_values(_after,
                                     ascending=False))
        other = cat_thres_df[N_TOP:].mean().rename('Other')
        cat_thres_df = cat_thres_df[:N_TOP].append(other)

        fig, ax = plt.subplots()
        (100*cat_thres_df[_before]).plot(markeredgecolor=COLOR_B,
                                         marker='o',
                                         color=COLOR_B,
                                         ax=ax,
                                         markerfacecolor=COLOR_B,
                                         linewidth=7.5)
        (100*cat_thres_df[_after]).plot(markeredgecolor=COLOR_A,
                                        marker='o',
                                        color=COLOR_A,
                                        ax=ax,
                                        markerfacecolor=COLOR_A,
                                        linewidth=7.5)
        ax.vlines(np.arange(len(cat_thres_df)),
                  ymin=len(cat_thres_df) * [0],
                  ymax=100*cat_thres_df[_after],
                  linestyle=':', linewidth=2)
        ax.set_xticks(np.arange(len(cat_thres_df)))
        ax.set_xticklabels(cat_thres_df.index, rotation=40, ha='right')
        ax.set_ylabel('Percentage of DL papers,\n'
                      'by arXiv CompSci category')
        ax.legend()
        dc.plot_to_s3(STATIC_FILES_BUCKET, f'static/figure_4{test_label}.png', plt)

        # fifth chart - changes in specialisation before / after threshold (top n countries)
        dl_counts = df.groupby('institute_country')['is_dl'].count()
        # remove the bottom 10% of countries here
        top_countries = list(dl_counts.loc[dl_counts > dl_counts.quantile(0.25)].index)
        top_countries = df.institute_country.apply(lambda x: x in top_countries)

        # Only highly citated papers
        avg_citation_counts = df[['year','citation_count']].groupby('year').quantile(0.5)
        avg_citation_counts['citation_count'] = avg_citation_counts['citation_count'].apply(lambda x: x if x > 0 else 1)
        highly_cited = map(lambda x : dc.highly_cited(x, avg_citation_counts),
                           [row for _, row in df.iterrows()])
        highly_cited = np.array(list(highly_cited))
        if self.test:
            highly_cited = np.array([True]*len(df))

        # Min year threshold
        min_year = (df.year >= MIN_RCA_YEAR 
                    if not self.test
                    else df.year >= 2000)

        # Apply filters before calculating RCA
        top_df = df.loc[top_countries & highly_cited & min_year]
        logging.info(f'Got {len(top_df)} rows for RCA calculation.\n'
                     'Breakdown (ctry, cite, yr) = '
                     f'{sum(top_countries)}, '
                     f'{sum(highly_cited)}, {sum(min_year)}')
        before_year = top_df[f'before_{year_threshold}']
        logging.info("Before is DL = "
                     f"{sum(top_df.loc[before_year].is_dl)}")
        logging.info("After is DL = "
                     f"{sum(top_df.loc[~before_year].is_dl)}")

        # Calculate revealed comparative advantage
        pre_threshold_rca = dc.calculate_rca_by_country(
            top_df[top_df[f'before_{year_threshold}']],
            country_column='institute_country',
            commodity_column='is_dl')
        post_threshold_rca = dc.calculate_rca_by_country(
            top_df[~top_df[f'before_{year_threshold}']],
            country_column='institute_country',
            commodity_column='is_dl')
        rca_combined = (pd.merge(pre_threshold_rca, post_threshold_rca,
                                 left_index=True, right_index=True,
                                 suffixes=('_before', '_after'))
                        .rename(columns={'is_dl_before': _before,
                                         'is_dl_after': _after})
                        .sort_values(_after, ascending=False))
        
        top_dl_countries = list(top_df[['institute_country', 'is_dl']]
                                .groupby('institute_country')
                                .sum()
                                .sort_values('is_dl', 
                                             ascending=False)[:N_TOP]
                                .index)
        condition = rca_combined.index.isin(top_dl_countries)
        rca_combined_top = rca_combined[condition]
        fig, ax = plt.subplots()
        rca_combined_top[_before].plot(markeredgecolor=COLOR_B,
                                       marker='o',
                                       markersize=20,
                                       color='white',
                                       ax=ax,
                                       markerfacecolor=COLOR_B,
                                       linewidth=0)
        rca_combined_top[_after].plot(markeredgecolor=COLOR_A,
                                      marker='o',
                                      markersize=20,
                                      color='white',
                                      ax=ax,
                                      markerfacecolor=COLOR_A,
                                      linewidth=0)
        col = [COLOR_A if x > y else '#d18270'
               for x, y in zip(rca_combined_top[_after],
                               rca_combined_top[_before])]
        ax.vlines(np.arange(len(rca_combined_top)),
                  ymin=rca_combined_top[_before],
                  ymax=rca_combined_top[_after],
                  linestyle=':',
                  color=col,
                  linewidth=4)
        ax.hlines(y=1,
                  xmin=-0.5,
                  xmax=len(rca_combined_top)-0.5,
                  color='darkgrey',
                  linestyle='--',
                  linewidth=4)
        ax.set_xticks(np.arange(len(rca_combined_top)))
        ax.set_xlim(-1, len(rca_combined_top))
        ax.set_xticklabels(rca_combined_top.index, 
                           rotation=40, ha='right')
        ax.legend()
        ax.set_ylabel('Specialisation in Deep Learning\n'
                      'relative to other arXiv CompSci categories')
        ax.set_xlabel('')
        dc.plot_to_s3(STATIC_FILES_BUCKET, f'static/figure_5{test_label}.png', plt)

        # mark as done
        logging.warning("Task complete")
        self.output().touch()


class StandaloneAnalysisTask(AnalysisTask):
    date = luigi.DateParameter(default=dt.now())
    _routine_id = luigi.Parameter(default=f'StandaloneDLDC{dt.now()}')
    production = luigi.BoolParameter(default=False)
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter(default='MYSQLDB')
    db_config_path = luigi.Parameter(default='mysqldb.config')
    mag_config_path = luigi.Parameter(default=None)
    insert_batch_size = luigi.IntParameter(default=None)
    articles_from_date = luigi.Parameter(default=None)
    s3_path_prefix = luigi.Parameter(default=None)
    raw_data_path = luigi.Parameter(default=None)
    grid_task_kwargs = DictParameterPlus(default={})
    cherry_picked = luigi.Parameter(default=None)
    def requires(self):
        if self.production:
            self.test = False
        set_log_level(True)
        pass
        
