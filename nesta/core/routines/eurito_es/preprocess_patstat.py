import luigi
from nesta.packages.patstat.fetch_appln_eu import extract_data
from nesta.core.orms.patstat_eu import ApplnFamily
from datetime import datetime as dt

class PreprocessPatstatTask(luigi.Task):
    date = luigi.DateParameter(default=dt.now())
    test = luigi.BoolParameter(default=True)
    
    def output(self):
        # something

    def run():
        data = extract_data(limit=1000 if self.test else None)
        insert_data('MYSQLDB', 'mysqldb', database,
                    Base, ApplnFamily, data, low_memory=True)
        output.touch()
