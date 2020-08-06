import pytest
from unittest import mock
from datetime import datetime as dt

from nesta.core.batchables.general.companies.curate.run import reformat_row
from nesta.core.batchables.general.companies.curate.run import sqlalchemy_to_dict
from nesta.core.batchables.general.companies.curate.run import retrieve_categories

def test_reformat_row():
    row = {'legal_name': 'joel corp',
           'alias1': 'joeljoel',
           'alias2': None,
           'alias3': 'joljol',
           'country_alpha_2': 'HK',
           'latitude': 12.3,
           'longitude': 23.4,
           'updated_at': dt.strptime('2019-08-14 09:53:10', '%Y-%m-%d %H:%M:%S'),
           'state_code': 'CA',
           'continent': 'NA',
           'long_text': 'this is some text about about Mexico and Egypt'}
    investor_names = ['jk', 'jk', 'ak']
    categories = ['balancing', 'juggling']
    categories_groups_list = ['circus', 'physics']
    _row = reformat_row(row, investor_names, categories, categories_groups_list)
    
    assert _row == {'aliases': ['joel corp', 'joeljoel', 'joljol'],
                    'investor_names': ['ak', 'jk'],
                    'country_alpha_2': 'HK',
                    'coordinates': {'lat': 12.3, 'lon':23.4},
                    'is_eu': False,
                    'updated_at': '2019-08-14 09:53:10',
                    'state_code': 'CA',
                    'state_name': 'California',
                    'continent': 'NA',
                    'continent_name': 'North America',
                    'category_list': categories,
                    'category_groups_list': categories_groups_list,
                    'long_text':'this is some text about about Mexico and Egypt',
                    'country_mentions':['EG','MX']}

def test_sqlalchemy_to_dict():
    _row = mock.Mock()
    _row.Organization.__dict__ = {'org_name': 'joel corp', 'org_age': 23, 'irrelevant_info': 'blah'}
    _row.Geographic.__dict__ = {'country_name': 'joelland', 'other_irrelevant_info': 'blah',
                                'country_code': 'JK'}
    row = sqlalchemy_to_dict(_row, ['org_name', 'org_age'], ['country_name', 'country_code'])
    assert row == {'org_name': 'joel corp', 'org_age': 23,
                   'country_name': 'joelland', 'country_code': 'JK'}
    
def test_retrieve_categories():
    session = mock.Mock()
    _row = mock.Mock()
    cat1 = mock.Mock()
    cat2 = mock.Mock()
    cat1.name = 'juggling'
    cat2.name = 'balancing'
    cat1.category_groups_list = 'circus,physics'
    cat2.category_groups_list = None
    session.query().select_from().join().filter().all.return_value = [cat1, cat2]
    categories, groups_list = retrieve_categories(_row, session)
    
    assert categories == ['juggling', 'balancing'] #<-- unsorted
    assert groups_list == ['circus', 'physics']
