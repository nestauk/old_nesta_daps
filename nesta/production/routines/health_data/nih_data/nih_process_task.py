'''
NIH data collection and processing
==================================

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump. The routine
transfers the data into the MySQL database before
processing and indexing the data to ElasticSearch.
'''

import luigi
import datetime
import logging
from luigi.contrib.esindex import ElasticsearchTarget
import pandas as pd

from nih_collect_task import CollectTask
from nesta.packages.health_data.process_nih import _extract_date
from nesta.packages.health_data.process_nih import geocode_dataframe
from nesta.packages.health_data.process_nih import country_iso_code_dataframe
from nesta.packages.decorators.schema_transform import schema_transform
from nesta.production.orms.orm_utils import get_mysql_engine


class ProcessTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter()
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield CollectTask(date=self.date,
                          db_config_path=self.db_config_path,
                          production=self.production)

    def output(self):
        # to Elasticsearch. requires:
            # local config file
            # index and mapping set up in ES

        # '''Points to the input database target'''
        # update_id = "worldreporter-%s" % self._routine_id
        # db_config = misctools.get_config("es.config", "es")
        # return ElasticsearchTarget(update_id=update_id,
        #                            index=self.index,
        #                            doc_type=self.doc_type,
        #                            extra_elasticsearch_args={"scheme":"https"},
        #                            **db_config)
        pass

    # @schema_transform(mapping, from_key, to_key)
    def run(self):
        # collect data from database
        engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
        # TODO: are these all the columns we want in ES?
        cols = ['application_id', 'org_city', 'org_country',
                'project_start', 'project_end']
        df = pd.read_sql('nih_projects', engine, columns=cols).head(30)
        df.columns = [c.lstrip('org_') for c in df.columns]

        # For sense-checking later
        n = len(df)
        n_ids = len(set(df.application_id))

        # Geocode the dataframe
        df = geocode_dataframe(df)
        # clean start and end dates
        for col in ["project_start", "project_end"]:
            df[col] = df[col].apply(_extract_date)
        # append iso codes for country
        df = country_iso_code_dataframe(df)
        assert len(set(df.application_id)) == n_ids
        assert len(df) == n

        self.output().touch()
