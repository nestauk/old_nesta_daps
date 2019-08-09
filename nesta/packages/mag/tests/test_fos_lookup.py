from nesta.packages.mag.fos_lookup import split_ids
from nesta.packages.mag.fos_lookup import build_fos_lookup
import mock
from collections import namedtuple

class Tmp:
    def __init__(self, id, child_ids):
        self.id = id
        self.child_ids = child_ids
        self.name = f'name-of-{id}'

def test_split_ids():
    assert split_ids(None) == []
    assert split_ids('1,1,3,4') == [1,1,3,4]

@mock.patch('nesta.packages.mag.fos_lookup.db_session')
def test_build_fos_lookup(mocked_session):
    fields_of_study = [Tmp(i, ','.join(str(x) 
                                       for x in range(i+1,i+3)))
                       for i in range(0,5)]

    mocked_session.return_value.__enter__.return_value.query.return_value.filter.return_value.all.return_value = fields_of_study

    fos_lookup = build_fos_lookup(None)
    for (pid, cid), (pname, cname) in fos_lookup.items():
        assert pname == f'name-of-{pid}'
        assert cname == f'name-of-{cid}'
        assert pid != cid        

