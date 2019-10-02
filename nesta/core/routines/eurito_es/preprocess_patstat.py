import luigi
from nesta.packages.patstat.fetch_appln_eu import extract_data
from nesta.core.orms.patstat_eu import ApplnFamily
from datetime import datetime as dt

class PreprocessPatstatTask(luigi.Task):
    date = luigi.DateParameter(default=dt.now())
    test = luigi.BoolParameter(default=True)

    def output(self):
        '''Points to the output database engine'''
        db_config_path = os.environ['MYSQLDB']
        db_config = misctools.get_config(db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "EURITO_patstat_pre"  # Note, not a real table
        update_id = "EURITO_patstat_pre_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run():
        data = extract_data(limit=1000 if self.test else None)
        database = 'dev' if self.test else 'production'
        insert_data('MYSQLDB', 'mysqldb', database,
                    Base, ApplnFamily, data, low_memory=True)
        output.touch()
