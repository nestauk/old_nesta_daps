'''
AbstractYielder
===============

Iterator for extracting abstracts from the database,
row-by-row, extracted in memory-efficient chunks.
'''

import os
from nesta.production.orms.nih_orm import Base
from nesta.production.orms.nih_orm import Abstracts
from nesta.production.orms.orm_utils import get_mysql_engine
from sqlalchemy.orm import sessionmaker


class AbstractYielder:
    '''Iterator for extracting abstracts from the database,
    row-by-row, extracted in memory-efficient chunks.

    Args:
        config_filepath (str): Filepath of the DB configuration file.
                               If set to :obj:`None` (default), the
                               configuration will be acquired from the
                               :obj:`MYSQLDBCONF` environmental variable.
        database (str): Name of the database to use.
    '''
    def __init__(self, config_filepath=None, database="dev"):
        if config_filepath is not None:
            os.environ["MYSQLDBCONF"] = config_filepath
        self.database = database

    def __enter__(self):
        '''Set up the database connection, session and query stub.'''
        engine = get_mysql_engine("MYSQLDBCONF", "mysqldb",
                                  database=self.database)
        engine.execution_options(stream_results=True)
        Session = sessionmaker(engine)
        Base.metadata.create_all(engine)
        self.session = Session()
        self.query_stub = self.session.query(Abstracts).order_by(Abstracts.application_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''Clean up the connection'''
        self.session.close()


    def iterrows(self, chunksize=1000, first_application_id=0):
        '''Iterate through the abstracts, using memory efficient chunks

        Args:
            chunksize (int): Size of chunks to extract from the database.

        Yields:
            :obj:`tuple` of :obj:`(Abstracts.application_id, Abstracts.abstract_text`
        '''

        application_id = first_application_id
        ids = [0]
        while len(ids) > 0:
            application_id = max(ids)
            ids = []
            condition = Abstracts.application_id > application_id
            query = self.query_stub.filter(condition).limit(chunksize)
            for row in query.all():
                ids.append(row.application_id)
                yield row.application_id, row.abstract_text


if __name__ == "__main__":
    abstracts = {}
    with AbstractYielder() as ay:
        for application_id, abstract_text in ay.iterrows(chunksize=1000):
            abstracts[application_id] = abstract_text
            break
    print(abstracts)
