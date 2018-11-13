# Test dependencies
from unittest import mock
from unittest import TestCase
from nesta.production.orms.nih_orm import Base
from nesta.production.orms.nih_orm import Abstracts
from sqlalchemy.orm import sessionmaker
from nesta.production.orms.orm_utils import get_mysql_engine

# What we're actually testing
from nesta.packages.health_data.nih_abstract_yielder import AbstractYielder


class TestAbstractYielder(TestCase):
    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    n_abstracts = 10

    def setUp(self):
        Session = sessionmaker(self.engine)
        session = Session()
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        for i in range(0, self.n_abstracts):
            abstract = Abstracts(application_id=10*i,
                                 abstract_text="hello")
            session.add(abstract)
            session.commit()
            del abstract
        session.close()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    @mock.patch('nesta.packages.health_data.'
                'nih_abstract_yielder.get_mysql_engine')
    def test_abstracts_found(self, mocked_engine):
        mocked_engine.return_value = self.engine
        with AbstractYielder() as ay:
            i = 0
            for application_id, abstract_text in ay.iterrows():
                i += 1
                assert type(application_id) == int
                assert type(abstract_text) == str
        assert i == self.n_abstracts
