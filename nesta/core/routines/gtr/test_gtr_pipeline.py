import pytest
from unittest import mock

from nesta.core.routines.gtr.gtr_collect import get_range_by_weekday

@pytest.fixture
def total_pages():
    return 1000

@pytest.fixture
def pages_per_day():
    return 142

DATETIME = 'nesta.core.routines.gtr.gtr_collect.datetime'

@mock.patch(DATETIME)
def test_get_range_by_weekday(datetime, total_pages, pages_per_day):
    _total_pages = 0    
    for weekday in range(0, 6):
        datetime.date.today().weekday.return_value = weekday
        first_page = pages_per_day*weekday + 1
        last_page = first_page + pages_per_day
        first, last = get_range_by_weekday(total_pages)
        _pages_per_day = last - first
        _total_pages += _pages_per_day
        assert _pages_per_day == pages_per_day
        assert (first, last) == (first_page, last_page)

    # Sunday also includes the remainder
    weekday = 6
    datetime.date.today().weekday.return_value = weekday
    first_page = pages_per_day*weekday + 1    
    first, last = get_range_by_weekday(total_pages)
    _pages_per_day = last - first
    _total_pages += _pages_per_day
    assert _pages_per_day > pages_per_day  # <-- note, different from above
    assert (first, last) == (first_page, total_pages+1)  # <-- note, different from above

    # Check the totals add up
    assert _total_pages == total_pages
