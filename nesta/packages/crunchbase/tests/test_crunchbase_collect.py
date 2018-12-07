import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from nesta.packages.crunchbase.crunchbase_collect import rename_uuid_columns
from nesta.packages.crunchbase.crunchbase_collect import generate_composite_key


def test_rename_uuid_columns():
    test_df = pd.DataFrame({'uuid': [1, 2, 3],
                            'org_uuid': [11, 22, 33],
                            'other_id': [111, 222, 333]
                            })

    expected_result = pd.DataFrame({'id': [1, 2, 3],
                                    'other_id': [111, 222, 333],
                                    'org_id': [11, 22, 33]
                                    })

    assert_frame_equal(rename_uuid_columns(test_df), expected_result, check_like=True)


def test_generate_composite_key():
    assert generate_composite_key('London', 'United Kingdom') == 'london_united-kingdom'
    assert generate_composite_key('Paris', 'France') == 'paris_france'
    assert generate_composite_key('Name-with hyphen', 'COUNTRY') == 'name-with-hyphen_country'


def test_generate_composite_key_raises_error_with_invalid_input():
    with pytest.raises(ValueError):
        generate_composite_key(None, 'UK')

    with pytest.raises(ValueError):
        generate_composite_key('city_only')

    with pytest.raises(ValueError):
        generate_composite_key(1, 2)
