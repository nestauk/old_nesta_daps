import pytest
import time
from unittest import mock
import xml.etree.ElementTree as ET

from nesta.packages.arxiv.collect_arxiv import _arxiv_request
from nesta.packages.arxiv.collect_arxiv import request_token
from nesta.packages.arxiv.collect_arxiv import total_articles
from nesta.packages.arxiv.collect_arxiv import arxiv_batch
from nesta.packages.arxiv.collect_arxiv import xml_to_json


@pytest.fixture
def mock_response():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>2018-11-13T11:47:39Z</responseDate>
    <request verb="ListRecords" metadataPrefix="arXiv">http://export.arxiv.org/oai2</request>
    <ListRecords>
        <record>
            <header>
                <identifier>oai:arXiv.org:0704.0999</identifier>
                <datestamp>2007-05-23</datestamp>
                <setSpec>math</setSpec>
            </header>
            <metadata>
                <arXiv xmlns="http://arxiv.org/OAI/arXiv/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://arxiv.org/OAI/arXiv/ http://arxiv.org/OAI/arXiv.xsd">
                    <id>0704.0999</id>
                    <created>2007-04-07</created>
                    <updated>2007-06-21</updated>
                    <authors>
                        <author>
                            <keyname>Author</keyname>
                            <forenames>G.</forenames>
                        </author>
                    </authors>
                    <title>Generic character sheaves on disconnected groups and character values</title>
                    <categories>math.RT</categories>
                    <comments>12 pages</comments>
                    <msc-class>05C85; 05C70; 68R10; 05B35</msc-class>
                    <journal-ref>Phys.Rev.Lett.99:131803,2007</journal-ref>
                    <doi>10.1103/PhysRevLett.99.131803</doi>
                    <abstract>  This is the summary of the article. </abstract>
                </arXiv>
            </metadata>
        </record>
        <record>
            <header>
                <identifier>oai:arXiv.org:0704.1000</identifier>
                <datestamp>2008-11-26</datestamp>
                <setSpec>physics:hep-ex</setSpec>
            </header>
            <metadata>
                <arXiv xmlns="http://arxiv.org/OAI/arXiv/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://arxiv.org/OAI/arXiv/ http://arxiv.org/OAI/arXiv.xsd">
                    <id>0704.1000</id>
                    <created>2007-04-07</created>
                    <authors>
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
                    </authors>
                    <title> Measurement of something interesting</title>
                    <categories>hep-ex</categories>
                    <comments>6 pages, 4 figures, Submitted to Physical Review Letters</comments>
                    <report-no>BELLE-CONF-0702</report-no>
                    <abstract>This paper did something clever and interesting</abstract>
                </arXiv>
            </metadata>
        </record>
        <record>
            <header>
                <identifier>missing_meta</identifier>
                <datestamp>2088-01-01</datestamp>
                <setSpec>physics:hep-ex</setSpec>
            </header>
        </record>
        <resumptionToken cursor="0" completeListSize="1463679">3132962|1001</resumptionToken>
    </ListRecords>
</OAI-PMH>'''


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


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_request_token_returns_correct_token(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    assert request_token() == '3132962'


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_request_token_doesnt_override_delay(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    request_token()
    if 'delay' in mocked_request.call_args[1]:
        assert mocked_request.call_args[1]['delay'] >= 3


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_total_articles_returns_correct_amount(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    assert total_articles() == 1463679


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_total_articles_doesnt_override_delay(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    total_articles()
    if 'delay' in mocked_request.call_args[1]:
        assert mocked_request.call_args[1]['delay'] >= 3


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_arxiv_batch_extracts_required_fields(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444', 0)
    expected_fields = {'datestamp', 'id', 'created', 'updated', 'title', 'categories',
                       'journal-ref', 'doi', 'msc-class', 'abstract', 'authors'}
    assert set(batch[0]) == expected_fields


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_arxiv_batch_handles_missing_fields(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444', 0)
    expected_fields = {'datestamp', 'id', 'created', 'title', 'categories', 'abstract', 'authors'}
    assert set(batch[1]) == expected_fields


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_arxiv_batch_author_json_conversion(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444', 0)

    expected_authors = '[{"keyname": "Author", "forenames": "G."}]'
    assert batch[0]['authors'] == expected_authors

    expected_authors = ('[{"keyname": "AnotherAuthor", "forenames": "L. M.", "suffix": "II", '
                        '"affiliation": "CREST, Japan Science and Technology Agency"}, '
                        '{"keyname": "Surname", "forenames": "Some other"}, '
                        '{"keyname": "Collaboration", "forenames": "An important"}]')
    assert batch[1]['authors'] == expected_authors


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_arxiv_batch_ignores_articles_with_missing_metadata(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    batch, _ = arxiv_batch('111222444', 0)
    assert len(batch) == 2

    datestamps = {row['datestamp'] for row in batch}
    assert '2088-01-01' not in datestamps


@mock.patch('nesta.packages.arxiv.collect_arxiv._arxiv_request')
def test_arxiv_batch_returns_resumption_cursor(mocked_request, mock_response):
    mocked_request.return_value = ET.fromstring(mock_response)
    _, cursor = arxiv_batch('111222444', 0)
    assert cursor == 1001


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
