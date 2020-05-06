from collections import defaultdict, namedtuple
import datetime
import numpy as np
import pandas as pd
import pytest
from sqlalchemy.orm.exc import NoResultFound
import time
from unittest import mock
import xml.etree.ElementTree as ET

from nesta.packages.arxiv.collect_arxiv import all_article_ids
from nesta.packages.arxiv.collect_arxiv import _arxiv_request
from nesta.packages.arxiv.collect_arxiv import request_token
from nesta.packages.arxiv.collect_arxiv import total_articles
from nesta.packages.arxiv.collect_arxiv import arxiv_batch
from nesta.packages.arxiv.collect_arxiv import xml_to_json
from nesta.packages.arxiv.collect_arxiv import load_arxiv_categories
from nesta.packages.arxiv.collect_arxiv import _category_exists
from nesta.packages.arxiv.collect_arxiv import retrieve_arxiv_batch_rows
from nesta.packages.arxiv.collect_arxiv import retrieve_all_arxiv_rows
from nesta.packages.arxiv.collect_arxiv import extract_last_update_date
from nesta.packages.arxiv.collect_arxiv import BatchedTitles
from nesta.packages.arxiv.collect_arxiv import create_article_institute_links
from nesta.core.orms.arxiv_orm import Article
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub

ARXIV_ENDPOINT = 'http://export.arxiv.org/oai2'
_ARXIV_REQUEST = 'nesta.packages.arxiv.collect_arxiv._arxiv_request'


@pytest.fixture(scope='session')
def mock_response():
    test_file = find_filepath_from_pathstub('mocked_arxiv_response.xml')
    with open(test_file, mode='rb') as f:
        return f.read()


class TestArxivRequest:
    @mock.patch('nesta.packages.arxiv.collect_arxiv.requests.get')
    def test_arxiv_request_sends_valid_params(self, mocked_requests, mock_response):
        mocked_requests().text = mock_response

        _arxiv_request('test.url', delay=0, metadataPrefix='arXiv')
        assert mocked_requests.call_args[0] == ('test.url', )
        assert mocked_requests.call_args[1] == {'params': {'verb': 'ListRecords', 'metadataPrefix': 'arXiv'}}

        _arxiv_request('another_test.url', delay=0, resumptionToken='123456')
        assert mocked_requests.call_args[0] == ('another_test.url', )
        assert mocked_requests.call_args[1] == {'params': {'verb': 'ListRecords', 'resumptionToken': '123456'}}

    @mock.patch('nesta.packages.arxiv.collect_arxiv.requests.get')
    def test_arxiv_request_returns_xml_element(self, mocked_requests, mock_response):
        mocked_requests().text = mock_response
        response = _arxiv_request('test.url', delay=0, metadataPrefix='arXiv')
        assert type(response) == ET.Element

    @mock.patch('nesta.packages.arxiv.collect_arxiv.requests.get')
    def test_arxiv_request_implements_delay(self, mocked_requests, mock_response):
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

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_request_token_returns_correct_token(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        assert request_token() == '3132962'

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_request_token_doesnt_override_delay(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        request_token()
        if 'delay' in mocked_request.call_args[1]:
            assert mocked_request.call_args[1]['delay'] >= 3


class TestTotalArticles:
    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_total_articles_returns_correct_amount(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        assert total_articles() == 1463679

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_total_articles_doesnt_override_delay(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        total_articles()
        if 'delay' in mocked_request.call_args[1]:
            assert mocked_request.call_args[1]['delay'] >= 3


class TestArxivBatch:
    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_sends_resumption_token_if_provided(self, mocked_request):
        batch, _ = arxiv_batch('111222444|1')
        assert mocked_request.mock_calls[0] == mock.call(ARXIV_ENDPOINT,
                                                         resumptionToken='111222444|1')

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_sends_prefix_if_no_resumption_token_provided(self, mocked_request):
        batch, _ = arxiv_batch()
        assert mocked_request.mock_calls[0] == mock.call(ARXIV_ENDPOINT,
                                                         metadataPrefix='arXiv')

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_sends_kwargs_to_request_if_no_resumption_token_provided(self, mocked_request):
        batch, _ = arxiv_batch(until='2019-03-01')
        assert mocked_request.mock_calls[0] == mock.call(ARXIV_ENDPOINT,
                                                         metadataPrefix='arXiv',
                                                         until='2019-03-01')

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_ignores_kwargs_if_resumption_token_provided(self, mocked_request):
        batch, _ = arxiv_batch('11111111|2', _from='2019-03-01')
        assert mocked_request.mock_calls[0] == mock.call(ARXIV_ENDPOINT,
                                                         resumptionToken='11111111|2')

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_extracts_required_fields(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        batch, _ = arxiv_batch()
        expected_fields = {'datestamp', 'id', 'created', 'updated', 'title', 'categories',
                           'article_source', 'journal_ref', 'doi', 'msc_class', 'abstract', 'authors'}
        assert set(batch[0]) == expected_fields

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_handles_missing_fields(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        batch, _ = arxiv_batch()
        expected_fields = {'datestamp', 'id', 'title', 'categories', 'abstract', 'authors', 'article_source'}
        assert set(batch[1]) == expected_fields

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_author_json_conversion(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        batch, _ = arxiv_batch()

        expected_authors = '[{"keyname": "Author", "forenames": "G."}]'
        assert batch[0]['authors'] == expected_authors

        expected_authors = ('[{"keyname": "AnotherAuthor", "forenames": "L. M.", "suffix": "II", '
                            '"affiliation": "CREST, Japan Science and Technology Agency"}, '
                            '{"keyname": "Surname", "forenames": "Some other"}, '
                            '{"keyname": "Collaboration", "forenames": "An important"}]')
        assert batch[1]['authors'] == expected_authors

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_converts_categories_to_list(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        batch, _ = arxiv_batch()
        assert batch[0]['categories'] == ['math.PR', 'math.RT']
        assert batch[1]['categories'] == ['hep-ex']

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_converts_dates(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        batch, _ = arxiv_batch()
        assert {'datestamp', 'created', 'updated'} < batch[0].keys()
        assert {'created', 'updated'}.isdisjoint(batch[1].keys())

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_returns_resumption_cursor(self, mocked_request, mock_response):
        mocked_request.return_value = ET.fromstring(mock_response)
        _, reumption_token = arxiv_batch()
        assert reumption_token == '3132962|1001'

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_returns_none_token_when_no_token_returned(self,
                                                                   mocked_request):
        mock_response = b'''<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
        <ListRecords>
            <record>
                <header>
                    <identifier>oai:arXiv.org:0704.1000</identifier>
                    <datestamp>2008-11-26</datestamp>
                </header>
                <metadata>
                    <arXiv xmlns="http://arxiv.org/OAI/arXiv/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://arxiv.org/OAI/arXiv/ http://arxiv.org/OAI/arXiv.xsd">
                    </arXiv>
                </metadata>
            </record>
            <resumptionToken cursor="0" completeListSize="1463679"></resumptionToken>
        </ListRecords>
    </OAI-PMH>'''
        mocked_request.return_value = ET.fromstring(mock_response)
        _, reumption_token = arxiv_batch()
        assert reumption_token is None

    @mock.patch(_ARXIV_REQUEST, autospec=True)
    def test_arxiv_batch_raises_valueerror_when_no_rows_to_collect(self,
                                                                   mocked_request):
        mock_response = b'''<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
        <responseDate>2018-11-13T11:47:39Z</responseDate>
        <request verb="ListRecords" metadataPrefix="arXiv">http://export.arxiv.org/oai2</request>
    </OAI-PMH>'''
        mocked_request.return_value = ET.fromstring(mock_response)

        with pytest.raises(ValueError) as e:
            arxiv_batch()
        assert str(e.value) == "No new records to collect from arXiv"


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


class TestRetrieveArxivBatchRows:
    @mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
    def test_retrieve_arxiv_batch_rows_calls_arxiv_batch_correctly(self, mocked_batch):
        mocked_batch.side_effect = [('data', 'mytoken|2'),
                                    ('data', 'mytoken|4'),
                                    ('data', None)]

        list(retrieve_arxiv_batch_rows(0, 9999, 'mytoken'))
        assert mocked_batch.mock_calls == [mock.call('mytoken|0'),
                                           mock.call('mytoken|2'),
                                           mock.call('mytoken|4')]

    @mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
    def test_retrieve_arxiv_batch_rows_returns_all_rows_till_empty_token(self, mocked_batch):
        rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
        mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                    (rows[2:4], 'mytoken|4'),
                                    (rows[4:6], None)]

        result = list(retrieve_arxiv_batch_rows(0, 9999, 'mytoken'))
        assert result == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']

    @mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
    def test_retrieve_arxiv_batch_rows_stops_at_end_cursor(self, mocked_batch):
        rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
        mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                    (rows[2:4], 'mytoken|4'),
                                    (rows[4:6], 'mytoken|6')]

        result = list(retrieve_arxiv_batch_rows(0, 4, 'mytoken|1'))
        assert result == ['row1', 'row2', 'row3', 'row4']


class TestRetrieveAllArxivRows:
    @mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
    def test_retrieve_all_arxiv_rows_calls_arxiv_batch_correctly(self, mocked_batch):
        rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
        mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                    (rows[2:4], 'mytoken|4'),
                                    (rows[4:6], None)]

        list(retrieve_all_arxiv_rows())
        assert mocked_batch.mock_calls == [mock.call(None),
                                           mock.call('mytoken|2'),
                                           mock.call('mytoken|4')]

    @mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
    def test_retrieve_all_arxiv_rows_returns_all_rows(self, mocked_batch):
        rows = ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']
        mocked_batch.side_effect = [(rows[0:2], 'mytoken|2'),
                                    (rows[2:4], 'mytoken|4'),
                                    (rows[4:6], None)]

        result = list(retrieve_all_arxiv_rows())
        assert result == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6']

    @mock.patch('nesta.packages.arxiv.collect_arxiv.arxiv_batch', autospec=True)
    def test_retrieve_all_arxiv_rows_handles_no_records_to_collect(self, mocked_batch):
        mocked_batch.side_effect = ValueError

        result = list(retrieve_all_arxiv_rows())
        assert result == []


class TestLastUpdateDate:
    def test_last_update_date_extracts_latest_date(self):
        updates = ['ArxivIterativeCollect_2019-03-14',
                   'ArxivIterativeCollect_2019-03-13',
                   'ArxivIterativeCollect_2001-01-01']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2019, 3, 14)

        updates = ['ArxivIterativeCollect_2018-03-14',
                   'ArxivIterativeCollect_2019-03-13',
                   'ArxivIterativeCollect_2020-01-01']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2020, 1, 1)

        updates = ['ArxivIterativeCollect_2001-01-14',
                   'ArxivIterativeCollect_2001-02-03',
                   'ArxivIterativeCollect_2001-02-03']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2001, 2, 3)

    def test_last_update_date_ignores_invalid_updates(self):
        updates = ['ArxivIterativeCollect_2019-03-14',
                   'ArxivIterativeCollect_2019-03-13',
                   'ArxivIterativeCollect_2019-04-99']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2019, 3, 14)

        updates = ['ArxivIterativeCollect_2019.03.14',
                   'ArxivIterativeCollect_2019-03-13',
                   'ArxivIterativeCollect_2019']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2019, 3, 13)

        updates = ['ArxivIterativeCollect_2019-03-14',
                   'ArxivIterativeCollect_2019-03-13',
                   'ArxivIterativeCollection_2019-04-09']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2019, 3, 14)

        updates = ['ArxivIterativeCollect_2019-03-13',
                   'badArxivIterativeCollect_2019-03-14',
                   'ArxivIterativeCollect_2018-04-01']
        latest = extract_last_update_date('ArxivIterativeCollect', updates)
        assert latest == datetime.datetime(2019, 3, 13)

    def test_last_update_date_raises_valueerror_if_none_found(self):
        updates = []

        with pytest.raises(ValueError):
            extract_last_update_date('ArxivIterativeCollect', updates)


@pytest.fixture
def mocked_articles():
    def _mocked_articles(articles):
        """creates a list of Article instances from the supplied details"""
        return [mock.Mock(spec=Article, **a) for a in articles]
    return _mocked_articles


class TestBatchedTitles:
    @mock.patch('nesta.packages.arxiv.collect_arxiv.prepare_title', autospec=True)
    @mock.patch('nesta.packages.arxiv.collect_arxiv.split_batches', autospec=True)
    def test_batched_titles_returns_all_prepared_titles(self,
                                                        mocked_split_batches,
                                                        mocked_prepare_title,
                                                        mocked_articles):
        mocked_split_batches.return_value = iter([[1, 2, 3], [4, 5, 6]])  # mocking a generator

        mocked_articles = [mocked_articles([{'id': 1, 'title': 'title A'},
                                            {'id': 2, 'title': 'title B'},
                                            {'id': 3, 'title': 'title C'}]),
                           mocked_articles([{'id': 4, 'title': 'title D'},
                                            {'id': 5, 'title': 'title E'},
                                            {'id': 6, 'title': 'title F'}])]

        mocked_session = mock.Mock()
        mocked_session.query().filter().all.side_effect = mocked_articles

        mocked_prepare_title.side_effect = ('prepared title A',
                                            'prepared title B',
                                            'prepared title C',
                                            'prepared title D',
                                            'prepared title E',
                                            'prepared title F')

        batcher = BatchedTitles([1, 2, 3, 4, 5, 6], batch_size=3, session=mocked_session)
        result = sorted(list(batcher))
        assert result == ['prepared title A',
                          'prepared title B',
                          'prepared title C',
                          'prepared title D',
                          'prepared title E',
                          'prepared title F']

    @mock.patch('nesta.packages.arxiv.collect_arxiv.prepare_title', autospec=True)
    @mock.patch('nesta.packages.arxiv.collect_arxiv.split_batches', autospec=True)
    def test_batched_titles_generates_title_id_lookup(self,
                                                      mocked_split_batches,
                                                      mocked_prepare_title,
                                                      mocked_articles):
        mocked_split_batches.return_value = iter([[1, 2, 3, 4, 5, 6]])

        mocked_articles = [mocked_articles([{'id': x, 'title': 'dummy_title'}
                           for x in range(1, 7)])]

        mocked_session = mock.Mock()
        mocked_session.query().filter().all.side_effect = mocked_articles

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

        batcher = BatchedTitles([1, 2, 3, 4, 5, 6], batch_size=3, session=mocked_session)
        list(batcher)
        for title, ids in expected_result.items():
            assert ids == batcher[title]

    @mock.patch('nesta.packages.arxiv.collect_arxiv.prepare_title', autospec=True)
    @mock.patch('nesta.packages.arxiv.collect_arxiv.split_batches', autospec=True)
    def test_batched_titles_calls_split_batches_correctly(self,
                                                          mocked_split_batches,
                                                          mocked_prepare_title,
                                                          mocked_articles):
        mocked_split_batches.return_value = iter([[1, 2, 3, 4, 5, 6]])

        mocked_session = mock.Mock()
        mocked_session.query().filter().all.return_value = mocked_articles([{'id': 1, 'title': 'dummy_title'}])

        mocked_prepare_title.return_value = 'clean title A'

        batcher = BatchedTitles([1, 2, 3, 4], batch_size=2, session=mocked_session)
        list(batcher)
        assert mocked_split_batches.mock_calls == [mock.call([1, 2, 3, 4], 2)]


class TestArticleInstituteLinks:
    def test_data_is_returned_in_correct_format(self):
        score = np.float64(1.11111)
        article_id = 1

        links = create_article_institute_links(article_id, ['a'], score)
        expected_result = [{'article_id': 1,
                            'institute_id': 'a',
                            'is_multinational': False,
                            'matching_score': 1.11111}]
        assert links == expected_result

    def test_create_article_institute_links_converts_score_to_float(self):
        score = np.float64(1.11111)
        article_id = 1

        links = create_article_institute_links(article_id, ['a'], score)
        assert type(links[0]['matching_score']) == float

    def test_multinational_flag_is_set_correctly(self):
        score = np.float64(1.21111)
        article_id = 2

        links = create_article_institute_links(article_id, ['a'], score)
        assert links[0]['is_multinational'] is False

        links = create_article_institute_links(article_id, ['a', 'b', 'c'], score)
        assert links[0]['is_multinational'] is True

    def test_multiple_results_are_returned_for_multinationals(self):
        score = np.float64(1.31111)
        article_id = 2

        links = create_article_institute_links(article_id, ['a', 'b', 'c'], score)
        assert len(links) == 3


@mock.patch('nesta.packages.arxiv.collect_arxiv.db_session')
def test_all_article_ids(mocked_session):
    Tmp = namedtuple('Tmp',['id'])
    mocked_session().__enter__().query().limit.return_value = [Tmp(i) for i in range(0, 131)]*83
    ids = all_article_ids(None)
    assert len(ids) == 131
