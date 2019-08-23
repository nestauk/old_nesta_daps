from nesta.core.batchables.health_data.nih_dedupe.run import get_value
from nesta.core.batchables.health_data.nih_dedupe.run import extract_yearly_funds

def test_get_value():
    test_dict = {'a':1, 'b':None}
    assert get_value(test_dict, 'a') == test_dict['a']
    assert get_value(test_dict, 'b') == test_dict['b']
    assert get_value(test_dict, 'c') == None

def test_extract_yearly_funds_year_not_none():
    src = {'year_fiscal_funding': 2010,
           'cost_total_project': 10,
           'date_start_project': '10-20-1202',
           'date_end_project': '10-20-1232'}
    assert extract_yearly_funds(src) is not None
    assert type(extract_yearly_funds(src)) is list
    assert len(extract_yearly_funds(src)) == 1

    funds = extract_yearly_funds(src)[0]    
    assert type(funds) is dict
    assert funds['year'] == src['year_fiscal_funding']
    assert funds['cost_ref'] == src['cost_total_project']
    assert funds['start_date'] == src['date_start_project']
    assert funds['end_date'] == src['date_end_project']
    

def test_extract_yearly_funds_year_is_none():
    src = {'year_fiscal_funding': None,
           'cost_total_project': 10,
           'date_start_project': '10-20-1202',
           'date_end_project': '10-20-1202'}
    assert extract_yearly_funds(src) == []
