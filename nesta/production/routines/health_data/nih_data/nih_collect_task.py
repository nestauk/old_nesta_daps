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
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from sqlalchemy import exists as sql_exists

from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget

from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.world_reporter_orm import Base
from nesta.packages.health_data.collect_nih import get_data_urls
from nesta.packages.health_data.collect_nih import iterrows
from nesta.packages.health_data.process_nih import geocode_dataframe
from nesta.packages.health_data.process_nih import _extract_date


def exists(_class, **kwargs):
    statements = [getattr(_class, pkey.name) == kwargs[pkey.name]
                  for pkey in _class.__table__.primary_key.columns]
    return sql_exists().where(and_(*statements))


class CollectTask(luigi.Task):
    '''Scrape CSVs from the World ExPORTER site and dump the
    data in the MySQL server.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path: (str) The output database configuration
    '''
    date = luigi.DateParameter()
    db_config_path = luigi.Parameter()
    production = luigi.BoolParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = "production" if self.production else "dev"
        db_config["table"] = "NIH <dummy>"  # Note, not a real table
        update_id = "LatestDataToMySQL_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        '''Execute the World ExPORTER collection and dump the
        data in the database'''

        # Setup the database connectors
        engine = get_mysql_engine("MYSQLDB", "mysqldb", 
                                  "production" if self.production else "dev")
        Base.metadata.create_all(engine)

        # Iterate over all tabs
        for i in range(0, 6):
            logging.info("Extracting table {}...".format(i))
            title, urls = get_data_urls(i)
            table_name = "nih_{}".format(title.replace(" ","").lower())
            _class = get_class_by_tablename(Base, table_name)
            logging.info("Extracted table {}. Merging to database...".format(table_name))
            # Commit the data
            Session = sessionmaker(engine)
            session = Session()
            for url in urls:       
                objs = [_class(**row) for row in iterrows(url)
                        if len(row) > 0 and
                        not session.query(exists(_class, **row)).scalar()]
                session.bulk_save_objects(objs)
                session.commit()
                if not self.production:
                    break            
            session.close()

        # Mark the task as done
        self.output().touch()
