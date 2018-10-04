'''
World ExPORTER
==============

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump. The routine
transfers the data into the MySQL database before
processing and indexing the data to ElasticSearch.
'''

import luigi
import datetime
from luigihacks import misctools
from luigihacks.mysqldb import MySqlTarget
import logging

# LatestDataToMySQL dependencies
from production.orms.orm_utils import get_mysql_engine
from production.orms.orm_utils import get_class_by_tablename
from production.orms.world_reporter_orm import Base
from packages.health_data.world_exporter import get_data_urls
from packages.health_data.world_exporter import iterrows
from sqlalchemy.orm import sessionmaker


class LatestDataToMySQL(luigi.Task):
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
        db_config["table"] = "WorldExporter <dummy>"  # Note, not a real table
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
                for row in iterrows(url):
                    g = _class(**row)
                    session.merge(g)
                if not self.production:
                    break
            session.commit()
            session.close()

        # Mark the task as done
        self.output().touch()


class RootTask(luigi.WrapperTask):
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
        yield LatestDataToMySQL(date=self.date, 
                                db_config_path=self.db_config_path,
                                production=self.production)
