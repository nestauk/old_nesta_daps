from collections import defaultdict
import pandas as pd
import pytest
from sqlalchemy.orm.exc import NoResultFound
import time
from unittest import mock
import xml.etree.ElementTree as ET

from nesta.packages.arxiv.collect_arxiv import _arxiv_request
from nesta.packages.arxiv.collect_arxiv import request_token
from nesta.packages.arxiv.collect_arxiv import total_articles
from nesta.packages.arxiv.collect_arxiv import arxiv_batch
from nesta.packages.arxiv.collect_arxiv import xml_to_json
from nesta.packages.arxiv.collect_arxiv import load_arxiv_categories
from nesta.packages.arxiv.collect_arxiv import _category_exists
from nesta.packages.arxiv.collect_arxiv import retrieve_arxiv_batch_rows
from nesta.packages.arxiv.collect_arxiv import retrieve_all_arxiv_rows
from nesta.packages.arxiv.collect_arxiv import batched_titles
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub


@pytest.fixture(scope='session')
def mock_response():
    test_file = find_filepath_from_pathstub('mocked_arxiv_response.json')
    with open(test_file, mode='rb') as f:
        return f.read()


@mock.patch('nesta.packages.arxiv.collect_arxiv.requests.get')
def test_arxiv_request_sends_valid_params(mocked_requests, mock_response):
    mocked_requests().text = mock_response

    _arxiv_request('test.url', delay=0, metadataPrefix='arXiv')
    assert mocked_requests.call_args[0] == ('test.url', )
    assert mocked_requests.call_args[1] == {'params': {'verb': 'ListRecords', 'metadataPrefix': 'arXiv'}}

    _arxiv_request('another_test.url', delay=0, resumptionToken='123456')
    assert mocked_requests.call_args[0] == ('another_test.url', )
    assert mocked_requests.call_args[1] == {'params': {'verb': 'ListRecords', 'resumptionToken': '123456'}}


@mock.patch('nesta.packages.arxiv.collect_arxiv.requests.get')
def test_arxiv_request_returns_xml_element(mocked_requests, mock_response):
    mocked_requests().text = mock_response
    response = _arxiv_request('test.url', delay=0, metadataPrefix='arXiv')
    assert type(response) == ET.Element


@mock.patch('nesta.packages.arxiv.collect_arxiv.requests.get')
def test_arxiv_request_implements_delay(mocked_requests, mock_response):
    """Fair use policy specifies wait of 3 seconds between each request:
        https://arxiv.org/help/api/user-manual#Quickstart
    """
    mocked_requests().text = mock_response

    start_time = time.time()
    _arxiv_request('test.url', delay=1, metadataPrefix='arXiv')
    assert time.time() - start_time > 1

    start_time = time.time()
    _arxiv_request('test.url', delay=3, metadataPrefix='arXiv')
    assert time.time() - start_time > 3


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_request_token_returns_correct_token(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    assert request_token() == '3132962'


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_request_token_doesnt_override_delay(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    request_token()
    if 'delay' in mocked_request.call_args[1]:
        assert mocked_request.call_args[1]['delay'] >= 3


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_total_articles_returns_correct_amount(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    assert total_articles() == 1463679


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_total_articles_doesnt_override_delay(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    total_articles()
    if 'delay' in mocked_request.call_args[1]:
        assert mocked_request.call_args[1]['delay'] >= 3


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_sends_resumption_token_if_provided(mocked_request):
    batch, _ = arxiv_batch('111222444|1')
    assert mocked_request.mock_calls[0] == mock.call('http://export.arxiv.org/oai2',
                                                     resumptionToken='111222444|1')


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_sends_prefix_if_no_resumption_token_provided(mocked_request):
    batch, _ = arxiv_batch()
    assert mocked_request.mock_calls[0] == mock.call('http://export.arxiv.org/oai2',
                                                     metadataPrefix='arXiv')


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_sends_kwargs_to_request_if_no_resumption_token_provided(mocked_request):
    batch, _ = arxiv_batch(until='2019-03-01')
    assert mocked_request.mock_calls[0] == mock.call('http://export.arxiv.org/oai2',
                                                     metadataPrefix='arXiv',
                                                     until='2019-03-01')


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_ignores_kwargs_if_resumption_token_provided(mocked_request):
    batch, _ = arxiv_batch('11111111|2', _from='2019-03-01')
    assert mocked_request.mock_calls[0] == mock.call('http://export.arxiv.org/oai2',
                                                     resumptionToken='11111111|2')


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_extracts_required_fields(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444|1')
    expected_fields = {'datestamp', 'id', 'created', 'updated', 'title', 'categories',
                       'journal_ref', 'doi', 'msc_class', 'abstract', 'authors'}
    assert set(batch[0]) == expected_fields


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_handles_missing_fields(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444|1')
    expected_fields = {'datestamp', 'id', 'title', 'categories', 'abstract', 'authors'}
    assert set(batch[1]) == expected_fields


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_author_json_conversion(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444|1')

    expected_authors = '[{"keyname": "Author", "forenames": "G."}]'
    assert batch[0]['authors'] == expected_authors

    expected_authors = ('[{"keyname": "AnotherAuthor", "forenames": "L. M.", "suffix": "II", '
                        '"affiliation": "CREST, Japan Science and Technology Agency"}, '
                        '{"keyname": "Surname", "forenames": "Some other"}, '
                        '{"keyname": "Collaboration", "forenames": "An important"}]')
    assert batch[1]['authors'] == expected_authors


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_converts_categories_to_list(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444|1')
    assert batch[0]['categories'] == ['math.PR', 'math.RT']
    assert batch[1]['categories'] == ['hep-ex']


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_converts_dates(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444|1')
    assert {'datestamp', 'created', 'updated'} < batch[0].keys()
    assert {'created', 'updated'}.isdisjoint(batch[1].keys())


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_returns_resumption_cursor(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    _, reumption_token = arxiv_batch('111222444|1')
    assert reumption_token == '3132962|1001'


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request', autospec=True)
def test_arxiv_batch_returns_none_at_end_of_data(mocked_request):
    mock_response = b'''<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>2018-11-13T11:47:39Z</responseDate>
    <request verb="ListRecords" metadataPrefix="arXiv">http://export.arxiv.org/oai2</request>
    <ListRecords>
        <record>
            <header>
                <identifier>oai:arXiv.org:0704.1000</identifier>
                <datestamp>2008-11-26</datestamp>
                <setSpec>physics:hep-ex</setSpec>
            </header>
            <metadata>
                <arXiv xmlns="http://arxiv.org/OAI/arXiv/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://arxiv.org/OAI/arXiv/ http://arxiv.org/OAI/arXiv.xsd">
                    <id>0704.1000</id>
                    <created>2007-04-999</created>
                    <authors>
                        <author>
                            <keyname>Surname</keyname>
                            <forenames>Some other</forenames>
                        </author>
                    </authors>
                    <title> Measurement of something interesting</title>
                    <abstract>This paper did something clever and interesting</abstract>
                </arXiv>
            </metadata>
        </record>
        <resumptionToken cursor="0" completeListSize="1463679"></resumptionToken>
    </ListRecords>
</OAI-PMH>'''
    mocked_request.return_value = ET.fromstring(mock_response)
    _, reumption_token = arxiv_batch('111222444|1')
    assert reumption_token is None


def test_xml_to_json_conversion():
    test_xml = '''<?xml version="1.0" encoding="UTF-8"?>
                    <authors xmlns="http://arxiv.org/OAI/arXiv/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://arxiv.org/OAI/arXiv/ http://arxiv.org/OAI/arXiv.xsd">
                        <author>
                            <keyname>AnotherAuthor</keyname>
                            <forenames>L. M.</forenames>
                            <suffix>II</suffix>
                            <affiliation>CREST, Japan Science and Technology Agency</affiliation>
                        </author>
                        <author>
                            <keyname>Surname</keyname>
                            <forenames>Some other</forenames>
                        </author>
                        <author>
                            <keyname>Collaboration</keyname>
                            <forenames>An important</forenames>
                        </author>
                    </authors>'''

    expected_output = ('[{"keyname": "AnotherAuthor", "forenames": "L. M.", "suffix": "II", '
                       '"affiliation": "CREST, Japan Science and Technology Agency"}, '
                       '{"keyname": "Surname", "forenames": "Some other"}, '
                       '{"keyname": "Collaboration", "forenames": "An important"}]')

    root = ET.fromstring(test_xml)
    assert xml_to_json(root, 'author', '{http://arxiv.org/OAI/arXiv/}') == expected_output


def test__category_exists_returns_correct_bool():
    mocked_session = mock.Mock()
    mocked_session.query().filter().one.return_value = 'found it'
    assert _category_exists(mocked_session, 1) is True

    mocked_session.query().filter().one.side_effect = NoResultFound
    assert _category_exists(mocked_session, 2) is False


@mock.patch('nesta.packages.arxiv.collect_arxiv._add_category', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv._category_exists', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.get_mysql_engine')
@mock.patch('nesta.packages.arxiv.collect_arxiv.try_until_allowed', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.pd', autospec=True)
def test_arxiv_categories_retrieves_and_appends_to_db(mocked_pandas, mocked_try,
                                                      mocked_engine, mocked_exists,
                                                      mocked_add):
    test_df = pd.DataFrame({'id': ['cs.IR', 'cs.IT', 'cs.LG'],
                            'description': ['Information Retrieval',
                                            'Information Theory',
                                            'Machine Learning']})
    mocked_pandas.read_csv.return_value = test_df

    mocked_session = mock.Mock()
    mocked_try.side_effect = [None, None, mocked_session]

    mocked_exists.side_effect = [True, False, False]
    expected_calls = [mock.call(mocked_session, cat_id='cs.IT', description='Information Theory'),
                      mock.call(mocked_session, cat_id='cs.LG', description='Machine Learning')]

    load_arxiv_categories('config', 'db', 'bucket', 'file')
    assert mocked_add.mock_calls == expected_calls


@mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
def test_retrieve_arxiv_batch_rows_calls_arxiv_batch_correctly(mocked_batch):
    mocked_batch.side_effect = [('data', 'mytoken|2'),
                                ('data', 'mytoken|4'),
                                ('data', None)]

    list(retrieve_arxiv_batch_rows(0, 9999, 'mytoken'))
    assert mocked_batch.mock_calls == [mock.call('mytoken|0'),
                                       mock.call('mytoken|2'),
                                       mock.call('mytoken|4')]


@mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
def test_retrieve_arxiv_batch_rows_returns_all_rows_till_empty_token(mocked_batch):
    rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
    mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                (rows[2:4], 'mytoken|4'),
                                (rows[4:6], None)]

    result = list(retrieve_arxiv_batch_rows(0, 9999, 'mytoken'))
    assert result == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']


@mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
def test_retrieve_arxiv_batch_rows_stops_at_end_cursor(mocked_batch):
    rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
    mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                (rows[2:4], 'mytoken|4'),
                                (rows[4:6], 'mytoken|6')]

    result = list(retrieve_arxiv_batch_rows(0, 4, 'mytoken|1'))
    assert result == ['row1', 'row2', 'row3', 'row4']


@mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
def test_retrieve_all_arxiv_batch_rows_calls_arxiv_batch_correctly(mocked_batch):
    rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
    mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                (rows[2:4], 'mytoken|4'),
                                (rows[4:6], None)]

    list(retrieve_all_arxiv_rows())
    assert mocked_batch.mock_calls == [mock.call(None),
                                       mock.call('mytoken|2'),
                                       mock.call('mytoken|4')]


@mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
def test_retrieve_all_arxiv_batch_rows_returns_all_rows(mocked_batch):
    rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
    mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                (rows[2:4], 'mytoken|4'),
                                (rows[4:6], None)]

    result = list(retrieve_all_arxiv_rows())
    assert result == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']


@mock.patch('nesta.packages.arxiv.collect_arxiv.prepare_title', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.split_batches', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.db_session', autospec=True)
def test_batched_titles_returns_all_prepared_titles(mocked_db_session,
                                                    mocked_split_batches,
                                                    mocked_prepare_title):
    title_id_lookup = defaultdict(list)
    mocked_split_batches.return_value = iter([[1, 2, 3], [4, 5, 6]])  # mocking a generator
    mocked_session = mock.Mock()
    mocked_session.query().filter().all.side_effect = ([(1, 'title A'),
                                                        (2, 'title B'),
                                                        (3, 'title C')],
                                                       [(4, 'title D'),
                                                        (5, 'title E'),
                                                        (6, 'title F')])
    mocked_db_session('dummy_arg').__enter__.return_value = mocked_session
    mocked_prepare_title.side_effect = ('prepared title A',
                                        'prepared title B',
                                        'prepared title C',
                                        'prepared title D',
                                        'prepared title E',
                                        'prepared title F')

    result = sorted(list(batched_titles(None, title_id_lookup, 3, None)))
    assert result == ['prepared title A',
                      'prepared title B',
                      'prepared title C',
                      'prepared title D',
                      'prepared title E',
                      'prepared title F']


@mock.patch('nesta.packages.arxiv.collect_arxiv.prepare_title', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.split_batches', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.db_session', autospec=True)
def test_batched_titles_generates_title_id_lookup(mocked_db_session,
                                                  mocked_split_batches,
                                                  mocked_prepare_title):
    title_id_lookup = defaultdict(list)
    mocked_split_batches.return_value = iter([[1, 2, 3], [4, 5, 6]])
    mocked_session = mock.Mock()
    mocked_session.query().filter().all.side_effect = ([(1, 'dummy title'),
                                                        (2, 'dummy title'),
                                                        (3, 'dummy title')],
                                                       [(4, 'dummy title'),
                                                        (5, 'dummy title'),
                                                        (6, 'dummy title')])
    mocked_db_session('dummy_arg').__enter__.return_value = mocked_session
    mocked_prepare_title.side_effect = ('clean title A',
                                        'clean title B',
                                        'clean title B',
                                        'clean title C',
                                        'clean title A',
                                        'clean title B')

    expected_result = defaultdict(list)
    expected_result.update({'clean title A': [1, 5],
                            'clean title B': [2, 3, 6],
                            'clean title C': [4]})

    list(batched_titles(None, title_id_lookup, 3, None))  # exhaust the generator
    assert title_id_lookup == expected_result


@mock.patch('nesta.packages.arxiv.collect_arxiv.prepare_title', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.split_batches', autospec=True)
@mock.patch('nesta.packages.arxiv.collect_arxiv.db_session', autospec=True)
def test_batched_titles_calls_split_batches_correctly(mocked_db_session,
                                                      mocked_split_batches,
                                                      mocked_prepare_title):
    title_id_lookup = defaultdict(list)
    mocked_split_batches.return_value = iter([[1, 2, 3], [4, 5, 6]])
    mocked_session = mock.Mock()
    mocked_session.query().filter().all.return_value = [(1, 'dummy title')]
    mocked_db_session('dummy_arg').__enter__.return_value = mocked_session
    mocked_prepare_title.return_value = 'clean title A'

    ids = [1, 2, 3, 4]
    batch_size = 2
    list(batched_titles(ids, title_id_lookup, batch_size, None))  # exhaust the generator
    assert mocked_split_batches.mock_calls == [mock.call([1, 2, 3, 4], 2)]
