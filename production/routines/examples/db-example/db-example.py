import luigi
import datetime
from luigihacks import misctools
from luigihacks.mysqldb import MySqlTarget

class InputData(luigi.Task):
    date = luigi.DateParameter()
    db_config = luigi.DictParameter()

    def output(self):
        update_id = self.db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.db_config)
    
    def run(self):
        self.output().touch()


class SomeTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.datetime.today())
    max_age = luigi.IntParameter()
    in_db_config = luigi.DictParameter()
    out_db_config = luigi.DictParameter()
    
    def requires(self):
        return InputData(date=self.date, db_config=self.in_db_config)

    def output(self):
        update_id = self.out_db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.out_db_config)

    def run(self):
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
    date = luigi.DateParameter(default=datetime.date.today())
    def requires(self):
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
