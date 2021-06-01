from unittest import mock
from io import BytesIO
from nesta.packages.cord.cord import (
    TemporaryFile,
    stream_to_file,
    cord_data,
    most_recent_date,
    remove_private_chars,
    convert_date,
    to_arxiv_format,
)

PATH = "nesta.packages.cord.cord.{}"

COMPACT_FILE = (
    "https://nesta-open-data.s3.eu-west-2.amazonaws.com/"
    "unit_tests/cord-19_2020-03-13.tar.gz"
)


@mock.patch(PATH.format("requests"))
def test_stream_to_file(mocked_requests):
    # Setup the mock stream
    response_text = b"this is some data"
    mocked_requests.get().__enter__().raw = BytesIO(response_text)
    # Steam the response to file
    with TemporaryFile() as tf:
        stream_to_file("dummy.com", tf)
        assert tf.read() == response_text


@mock.patch(PATH.format("DATA_URL"))
def test_cord_data(mocked_data_url):
    mocked_data_url.format.return_value = COMPACT_FILE
    data = list(cord_data(date="2020-03-13"))
    assert len(data) > 1000  # Lots of data
    first = data[1000]  # Not the first row, but one that contains good data
    assert len(first) > 5  # Lots of columns
    # All columns have the same length
    assert all(len(row) == len(first) for row in data)
    assert type(first["abstract"]) is str  # Contains text data
    assert len(first["abstract"]) > 100  # Contains text data


def test_most_recent_date():
    year, month, day = most_recent_date().split("-")
    assert int(year) >= 2021
    assert int(month) > 0
    assert int(day) > 0


def test_remove_private_chars():
    before = "\uf0b7test \uf0c7string 😃"
    after = remove_private_chars(before)
    assert after == "test string 😃"


def test_convert_date():
    assert convert_date("1991") == "1991-01-01"
    assert convert_date("1991-03-31") == "1991-03-31"
    assert convert_date("") is None


def test_to_arxiv_format():
    cord_row = {
        "abstract": "An abstract",
        "title": "a Title",
        "authors": "name, surname; other name, other surname",
        "cord_uid": 123,
        "a bonus field": "drop me",  # will be dropped
        "publish_time": "2020-02-12",
        "journal": "JOURNAL",
        "doi": "",  # converted to None
        "another bonus field": None,  # will be dropped
    }
    arxiv_row = {
        "id": "cord-123",
        "abstract": "An abstract",
        "title": "a Title",
        "authors": ["name, surname", "other name, other surname"],
        "created": "2020-02-12",
        "datestamp": "2020-02-12",
        "updated": "2020-02-12",
        "journal_ref": "JOURNAL",
        "doi": None,
        "article_source": "cord",  # hard-coded
    }
    assert to_arxiv_format(cord_row) == arxiv_row
