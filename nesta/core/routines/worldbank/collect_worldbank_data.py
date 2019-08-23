'''
Collect WorldBank
==================

Collect WorldBank data for as many countries as possibles by
automatically discovering data based on a given set of variable codes.
For example, as given variable can appear in multiple datasets so any
missing values can be partially recovered by considering all datasets.
'''
import luigi
import datetime
import json
import logging
from collections import defaultdict
from sqlalchemy.orm import sessionmaker

from nesta.core.luigihacks import autobatch
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks import s3
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub

from nesta.packages.worldbank.collect_worldbank import get_variables_by_code
from nesta.packages.worldbank.collect_worldbank import get_country_data_kwargs
from nesta.packages.worldbank.collect_worldbank import get_worldbank_resource
from nesta.packages.worldbank.collect_worldbank import flatten_country_data
from nesta.packages.worldbank.collect_worldbank import clean_variable_names
from nesta.packages.worldbank.collect_worldbank import discover_variable_name
from nesta.core.orms.worldbank_orm import WorldbankCountry
from nesta.core.orms.worldbank_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine


S3PREFIX = "s3://nesta-dev/production_batch_example_"


class WorldbankTask(autobatch.AutoBatchTask):
    '''Get Worldbank data by hitting the API in batches.'''
    date = luigi.DateParameter()
    db_config = luigi.DictParameter()
    variable_codes = luigi.ListParameter()

    def output(self):
        '''Points to the MySqlTarget checkpoint'''
        update_id = self.db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Set the number of pages if in test mode
        max_pages = None
        if self.test:
            max_pages = 2

        # Generate the run parameters
        variables = get_variables_by_code(self.variable_codes)
        aliases = {series: discover_variable_name(series)
                   for series, sources in variables.items()}
        kwargs_list = get_country_data_kwargs(variables=variables,
                                              aliases=aliases,
                                              max_pages=max_pages)

        # Add the mandatory `outinfo' and `done' fields
        job_params = []
        s3fs = s3.S3FS()
        for i, kwargs in enumerate(kwargs_list):
            params = dict(**kwargs)
            outfile = ("s3://nesta-production-intermediate/"
                       f"{self.job_name}-{i}")
            params["outinfo"] = outfile
            params["done"] = s3fs.exists(outfile)
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''

        # Retrieve the batched data
        country_data = defaultdict(dict)
        n_rows = 0
        for i, params in enumerate(job_params):
            print(i, " of ", len(job_params))
            _body = s3.S3Target(params["outinfo"]).open("rb")
            _country_data = json.loads(_body.read().decode('utf-8'))
            for country, data in _country_data.items():
                for var_name, data_row in data.items():
                    n_rows += 1
                    country_data[country][var_name] = data_row
        print(f"Got {n_rows} rows of data")

        # Merge with metadata, then flatten and clean
        country_metadata = get_worldbank_resource("countries")
        flat_country_data = flatten_country_data(country_data,
                                                 country_metadata)
        cleaned_data = clean_variable_names(flat_country_data)

        # Commit the data
        engine = get_mysql_engine("MYSQLDB", "mysqldb",
                                  self.db_config['database'])
        Base.metadata.create_all(engine)
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
        logging.getLogger().setLevel(logging.INFO)

        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if self.production else "dev"
        db_config["table"] = "worldbank_countries"
        
        variable_codes = ["SP.RUR.TOTL.ZS", "SP.URB.TOTL.IN.ZS"
                          "SP.POP.DPND", "SP.POP.TOTL",
                          "SP.DYN.LE00.IN", "SP.DYN.IMRT.IN",
                          "BAR.NOED.25UP.ZS", "BAR.TER.CMPT.25UP.ZS",
                          "NYGDPMKTPSAKD", "SI.POV.NAHC", "SI.POV.GINI"]


        job_name=(f"Worldbank-{self.date}-"
                  f"{'_'.join(variable_codes).replace('.','_')}-"
                  f"{self.production}")[0:120]

        yield WorldbankTask(date=self.date, db_config=db_config, 
                            variable_codes=variable_codes,
                            batchable=find_filepath_from_pathstub("core/batchables/collect_worldbank/"),
                            env_files=[find_filepath_from_pathstub("/nesta/nesta"),
                                       find_filepath_from_pathstub("/config/mysqldb.config")],
                            job_def="py36_amzn1_image",
                            job_name=job_name,
                            job_queue="HighPriority",
                            region_name="eu-west-2",
                            poll_time=10,
                            max_live_jobs=200,
                            test=(not self.production))

