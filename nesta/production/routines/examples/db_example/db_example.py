'''
Database example
================

An example of building a pipeline with database Targets
'''

import luigi
import datetime
from luigihacks import misctools
from luigihacks.mysqldb import MySqlTarget


class InputData(luigi.Task):
    '''Dummy task acting as the single input data source.

    Args:
        date (datetime): Date used to label the outputs
        db_config: (dict) The input database configuration
    '''    
    date = luigi.DateParameter()
    db_config = luigi.DictParameter()

    def output(self):
        '''Points to the input database target'''
        update_id = self.db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.db_config)

    def run(self):
        '''Example of marking the update table'''
        self.output().touch()


class SomeTask(luigi.Task):
    '''Task which increments the age of the muppets, by first selecting
    muppets with an age less than `max_age`.

    Args:
        date (datetime): Date used to label the outputs
        max_age (int): Maximum age of muppets to select from the database
        in_db_config: (dict) The input database configuration
        out_db_config: (dict) The output database configuration
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    max_age = luigi.IntParameter()
    in_db_config = luigi.DictParameter()
    out_db_config = luigi.DictParameter()

    def requires(self):
        '''Gets the input database engine'''
        return InputData(date=self.date, db_config=self.in_db_config)

    def output(self):
        '''Points to the output database engine'''        
        update_id = self.out_db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.out_db_config)

    def run(self):
        '''Increments the muppets' ages by 1'''
        # Extract the data from the input connection
        cnx = self.input().connect()
        cursor = cnx.cursor()
        query = ("SELECT name, age FROM muppets_input WHERE age <= %s")
        cursor.execute(query, (self.max_age, ))
        data = [dict(name=name, age=age+1) for name, age in cursor]
        cursor.close()
        cnx.close()

        # Write the data to the output connection
        cnx = self.output().connect(autocommit=False)
        cursor = cnx.cursor()
        insert = ("INSERT INTO muppets_output "
                  "(name, age) VALUES (%(name)s, %(age)s) "
                  "ON DUPLICATE KEY UPDATE age=%(age)s")
        for row in data:
            print(row)
            cursor.execute(insert, row)

        # Commit, close and update the marker table
        cnx.commit()
        cursor.close()
        cnx.close()
        self.output().touch()


class RootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "dev"

        # Prepare the input DB config
        in_db_config = db_config.copy()
        in_db_config["table"] = "muppets_input"

        # Prepare the output DB config
        out_db_config = db_config.copy()
        out_db_config["table"] = "muppets_output"

        yield SomeTask(date=self.date, max_age=40,
                       in_db_config=in_db_config,
                       out_db_config=out_db_config)
