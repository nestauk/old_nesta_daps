"""
UK Geographic Lookup
====================

Routine to collect all available administrative lookups between 
UK geographic entities.
"""

from nesta.packages.geographies.uk_geography_lookup import get_gss_codes
from nesta.packages.geographies.uk_geography_lookup import get_children

from nesta.production.orms.orm_utils import insert_data
from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.uk_geography_lookup_orm import Base

from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget

from collections import defaultdict
import datetime
import luigi

MYSQLDB_ENV = 'MYSQLDB'

class UkGeoLookupTask(luigi.Task):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if self.production else "dev"
        db_config["table"] = "UK Geography Lookup (dummy) "
        update_id = db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):

        # Get all UK geographies, and group by country and base 
        gss_codes = get_gss_codes()
        country_codes = defaultdict(lambda: defaultdict(list))
        for code in gss_codes:
            country = code[0]
            base = code[0:3]
            # Shortened test mode
            if not self.production and base not in ("S32", "S23"):
                continue
            country_codes[country][base].append(code)

        # Iterate through country and base                      
        output = []
        for country, base_codes in country_codes.items():
            # Try to find children for each base...             
            for base in base_codes.keys():
                for base_, codes in base_codes.items():
                    # ...except for the base of the parent      
                    if base == base_:
                        continue
                    output += get_children(base, codes)

        # Write to database
        _class = get_class_by_tablename(Base, "onsOpenGeo_geographic_lookup")
        objs = insert_data(MYSQLDB_ENV, "mysqldb",
                           "production" if self.production else "dev",
                           Base, _class, output)
        self.output().touch()
