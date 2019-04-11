"""
collect_ngrams
==============

Routine to collect N-grams from wiktionary
titles.
"""

import luigi
import datetime
from sqlalchemy.orm import sessionmaker

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks import misctools

from nesta.packages.wikipedia.wiktionary_ngrams import find_latest_wikidump
from nesta.packages.wikipedia.wiktionary_ngrams import extract_ngrams
from nesta.production.orms.wiktionary_ngrams_orm import WiktionaryNgram
from nesta.production.orms.wiktionary_ngrams_orm import Base
from nesta.production.orms.orm_utils import insert_data

MYSQLDB_ENV = 'MYSQLDB'


class CollectNgramTask(luigi.Task):
    """Local task to collect N-grams from wiktionary titles.
    
    Args:
        date (datetime): Datetime string parameter for checkpoint.
        db_config (str): Database configuration file.
    """

    date = luigi.DateParameter()
    db_config = luigi.DictParameter()
    test = luigi.BoolParameter(default=True)

    def output(self):
        '''Points to the output database engine''' 
        update_id = self.db_config["table"]+str(self.date)
        return MySqlTarget(update_id=update_id, **self.db_config)


    def run(self):
        '''Run the data collection'''
        #engine = get_mysql_engine(MYSQLDB_ENV, "mysqldb",
        #                          self.db_config['database'])
        #Base.metadata.create_all(engine)
        #Session = sessionmaker(engine)
        #session = Session()        
        wiki_date = find_latest_wikidump()
        ngrams = extract_ngrams(wiki_date)
        if self.test:
            ngrams = list(ngrams)[0:100]
        #for n in ngrams:
        #    ngram = WiktionaryNgram(ngram=n)
        #    session.add(ngram)
        #session.commit()
        #session.close()        
        insert_data(MYSQLDB_ENV, "mysqldb", self.db_config['database'], 
                    Base, WiktionaryNgram, [dict(ngram=n) for n in ngrams])

        
        self.output().touch()

class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if self.production else "dev"
        db_config["table"] = "wiktionary_ngrams"
        yield CollectNgramTask(date=self.date, db_config=db_config, test=not self.production)
