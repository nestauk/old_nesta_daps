import unittest
from sqlalchemy.orm import sessionmaker
from nesta.core.orms.wiktionary_ngrams_orm import WiktionaryNgram
from nesta.core.orms.wiktionary_ngrams_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine
from sqlalchemy.exc import IntegrityError


class TestWiktionaryNgram(unittest.TestCase):
    '''Check that the WiktionaryNgram ORM works as expected'''
    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    Session = sessionmaker(engine)

    def setUp(self):
        '''Create the temporary table'''
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        '''Drop the temporary table'''
        Base.metadata.drop_all(self.engine)

    def test_good_relation(self):
        session = self.Session()
        ngram = WiktionaryNgram(ngram="something")
        new_ngram = WiktionaryNgram(ngram="something")

        # Add the group and member
        session.add(ngram)
        session.commit()

        # Shouldn't be able to add duplicate data
        del ngram
        session.add(new_ngram)
        self.assertRaises(IntegrityError, session.commit)
        session.rollback()
        session.close()


if __name__ == "__main__":
    unittest.main()
