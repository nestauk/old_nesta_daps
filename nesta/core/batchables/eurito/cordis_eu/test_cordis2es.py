import pytest
from unittest import mock
import sys

from nesta.core.batchables.eurito.cordis_eu import run

PATH = 'nesta.core.batchables.eurito.cordis_eu.run.{}'

YEAR = 2019
START_DATE = f"{YEAR}-01-02"
END_DATE = f"{YEAR}-01-02"
TEXT = 'blah'
@pytest.fixture
def row():
    return {'start_date_code': f'{START_DATE}T00:00:00',
            'end_date_code': f'{END_DATE}T00:00:00',
            'project_description': TEXT,
            'objective': TEXT*2,
            'rcn': 100}

def _standard_text_test(row):
    assert f'\n{TEXT}\n' in row['description']
    assert row['description'].endswith(f'\n{TEXT*2}')
    assert row['link'].startswith('https')
    assert row['link'].endswith(str(row['rcn']))


def test_reformat_row_no_dates(row):
    row['start_date_code'] = None
    row['end_date_code'] = None
    row = run.reformat_row(row)
    assert row['start_date_code'] is None
    assert row['end_date_code'] is None
    assert row['year'] is None
    _standard_text_test(row)


def test_reformat_row_no_end_date(row):
    row['end_date_code'] = None
    row = run.reformat_row(row)
    assert row['start_date_code'] == START_DATE
    assert row['end_date_code'] is None
    assert row['year'] == YEAR
    _standard_text_test(row)


def test_reformat_row_no_start_date(row):
    row['start_date_code'] = None
    row = run.reformat_row(row)
    assert row['start_date_code'] is None
    assert row['end_date_code'] == END_DATE
    assert row['year'] == YEAR
    _standard_text_test(row)


def test_validate_date_null(row):
    row['start_date_code'] = None
    year = run.validate_date(row, 'start')
    assert year is None
    assert row['start_date_code'] is None


def test_validate_date_not_null(row):
    year = run.validate_date(row, 'start')
    assert year == YEAR
    assert row['start_date_code'] == START_DATE


def test_validate_date_bad_date(row):
    row['start_date_code'] = 'blah'
    year = run.validate_date(row, 'start')
    assert year is None
    assert row['start_date_code'] is None
