"""
collect_worldbank_data
======================

Routine to collect data via the Worldbank API.
"""

import luigi
import datetime
from sqlalchemy.orm import sessionmaker

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks import misctools

from nesta.packages.worldbank.collect_worldbank import get_variables_by_code
from nesta.packages.worldbank.collect_worldbank import get_country_data
from nesta.packages.worldbank.collect_worldbank import get_worldbank_resource
from nesta.packages.worldbank.collect_worldbank import flatten_country_data
from nesta.packages.worldbank.collect_worldbank import clean_variable_names
from nesta.production.orms.worldbank_orm import WorldbankCountry
from nesta.production.orms.worldbank_orm import Base
from nesta.production.orms.orm_utils import get_mysql_engine

MYSQLDB_ENV = 'MYSQLDB'


class CollectWorldbankTask(luigi.Task):
    """Local task to collect data via the Worldbank API.

    Args:
        date (datetime): Datetime string parameter for checkpoint.
        db_config (str): Database configuration file.
        variable_codes (list): List of Worldbank variable codes to collect.
    """

    date = luigi.DateParameter()
    db_config = luigi.DictParameter()
    variable_codes = luigi.ListParameter()

    def output(self):
        '''Points to the output database engine'''
        update_id = self.db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.db_config)

    def run(self):
        '''Run the data collection'''
        engine = get_mysql_engine(MYSQLDB_ENV, "mysqldb",
                                  self.db_config['database'])
        Base.metadata.create_all(engine)

        # Get the data
        variables = get_variables_by_code(self.variable_codes)
        country_data = get_country_data(variables)
        country_metadata = get_worldbank_resource("countries")
        flat_country_data = flatten_country_data(country_data,
                                                 country_metadata)
        cleaned_data = clean_variable_names(flat_country_data)

        # Commit the data
        Session = sessionmaker(engine)
        session = Session()
        for row in cleaned_data:
            country = WorldbankCountry(**row)
            session.add(country)
        session.commit()
        session.close()
        self.output().touch()


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if self.production else "dev"
        db_config["table"] = "worldbank_countries"

        variable_codes = ["SP.RUR.TOTL.ZS", "SP.URB.TOTL.IN.ZS",
                          "SP.POP.DPND", "SP.POP.TOTL",
                          "SP.DYN.LE00.IN", "SP.DYN.IMRT.IN",
                          "BAR.NOED.25UP.ZS",
                          "BAR.TER.CMPT.25UP.ZS",
                          "NYGDPMKTPSAKD",
                          "SI.POV.NAHC", "SI.POV.GINI"]

        if not self.production:
            variable_codes = variable_codes[0:2]

        yield CollectWorldbankTask(date=self.date, db_config=db_config,
                                   variable_codes=variable_codes)

