'''
ESCO data collection (dummy routine)
====================================
Luigi routine to write ESCO data into the database
'''
from datetime import datetime
import luigi
import logging
import os

from nesta.packages.reviews import esco_loader
from nesta.packages.misc_utils.batches import BatchWriter
from nesta.core.orms.esco_orm import Base, Occupations, Skills, OccupationSkills
from nesta.core.orms.orm_utils import get_mysql_engine, db_session
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget

class RootTask(luigi.WrapperTask):
    '''The root task, which collects the supplied parameters and calls the CollectESCOTask.

    Args:
        date (datetime): Date used to label the outputs
        name (str): name to search for in the swapi
        production (bool): test mode or production mode
    '''
    date = luigi.DateParameter(default=datetime.today())
    production = luigi.BoolParameter(default=False)
    name = luigi.Parameter(default=f"ESCO_dummy_task-{date}-{not production}")
    db_config_path = luigi.Parameter(default="mysqldb.config")
    insert_batch_size = luigi.IntParameter(default=500)
    s3_bucket_path = luigi.Parameter(default="")

    def requires(self):
        '''Call the task to run before this in the pipeline.'''

        logging.getLogger().setLevel(logging.INFO)
        return CollectESCOTask(date=self.date,
                               name=self.name,
                               test=not self.production,
                               db_config_env='MYSQLDB',
                               db_config_path=self.db_config_path,
                               insert_batch_size=self.insert_batch_size,
                               s3_bucket_path = self.s3_bucket_path
                               )

class CollectESCOTask(luigi.Task):
    '''Collect ESCO data and store in the MySQL server.
    Args:
        date (datetime): Datetime used to label the outputs
        name (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): number of records to insert into the database at once
        articles_from_date (str): new and updated articles from this date will be
                                  retrieved. Must be in YYYY-MM-DD format
    '''
    date = luigi.DateParameter()
    name = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    s3_bucket_path = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "ESCO <dummy>"  # Note, not a real table
        update_id = "CollectESCO_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):

        logging.info(f"Starting the CollectESCO task...")

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        with db_session(self.engine) as session:

            Base.metadata.create_all(self.engine)
            logging.info(f"Created the tables")

            occupation_count = 0
            skill_count = 0
            links_count = 0

            # use BatchWriter, which collects a list and automatically sends it
            # to the database when the predefined list length is reached
            big_batch = BatchWriter(self.insert_batch_size,
                                    esco_loader.add_new_rows,
                                    session)

            # load occupations
            for row in esco_loader.load_csv_as_dict(self.s3_bucket_path + 'occupations.csv'):
                big_batch.append(Occupations(**row))
                occupation_count += 1
                if self.test and occupation_count == 10:
                    logging.warning("Limiting to 10 occupations while in test mode")
                    break

            # load skills
            for row in esco_loader.load_csv_as_dict(self.s3_bucket_path+'skills.csv'):
                big_batch.append(Skills(**row))
                skill_count += 1
                if self.test and skill_count == 10:
                    logging.warning("Limiting to 10 skills while in test mode")
                    break

            # load table linking occupations to skills
            for row in esco_loader.load_csv_as_dict(self.s3_bucket_path+'occupations_skills_link.csv'):
                big_batch.append(OccupationSkills(**row))
                links_count += 1
                if self.test and links_count == 10:
                    logging.warning("Limiting to 10 links while in test mode")
                    break

            # if any rows remaining in the batch, send them to the database
            if big_batch:
                big_batch.write()

            logging.info(f"Processed {occupation_count} occupations")
            logging.info(f"Processed {skill_count} skills")
            logging.info(f"Processed {links_count} links between occupations and skills")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
