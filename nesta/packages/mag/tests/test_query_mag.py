import mock
import pandas as pd

from nesta.packages.mag.query_mag import prepare_title
from nesta.packages.mag.query_mag import build_expr
from nesta.packages.mag.query_mag import query_mag_api
from nesta.packages.mag.query_mag import concatenate_ids
from nesta.packages.mag.query_mag import query_fields_of_study


def test_prepare_title_removes_extra_spaces():
    assert prepare_title("extra   spaces       here") == "extra spaces here"
    assert prepare_title("trailing space ") == "trailing space"


def test_prepare_title_replaces_invalid_characters_with_single_space():
    assert prepare_title("invalid%&*^˙∆¬stuff") == "invalid stuff"
    assert prepare_title("ba!!!!d3") == "ba d3"


def test_prepare_title_lowercases():
    assert prepare_title("UPPER") == "upper"


def test_build_expr_correctly_forms_query():
    assert list(build_expr([1, 2], 'Id', 1000)) == ["expr=OR(Id=1,Id=2)"]
    assert list(build_expr(['cat', 'dog'], 'Ti', 1000)) == ["expr=OR(Ti='cat',Ti='dog')"]


def test_build_expr_respects_query_limit_and_returns_remainder():
    assert list(build_expr([1, 2, 3], 'Id', 21)) == ["expr=OR(Id=1,Id=2)", "expr=OR(Id=3)"]


@mock.patch('nesta.packages.mag.query_mag.requests.post')
def test_query_mag_api_sends_correct_request(mocked_requests):
    sub_key = 123
    fields = ['Id', 'Ti']
    expr = "expr=OR(Id=1,Id=2)"
    query_mag_api(expr, fields, sub_key, query_count=10, offset=0)
    expected_call_args = mock.call(
        "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate",
        data=b"expr=OR(Id=1,Id=2)&count=10&offset=0&attributes=Id,Ti",
        headers={'Ocp-Apim-Subscription-Key': 123,
                 'Content-Type': 'application/x-www-form-urlencoded'})
    assert mocked_requests.call_args == expected_call_args


def test_concatenate_ids_converts_to_a_string():
    assert concatenate_ids([1, 2, 3]) == '1,2,3'
    assert concatenate_ids(['cat', 'dog', 'frog']) == 'cat,dog,frog'


def test_concatenate_ids_returns_none_when_ids_is_nan():
    assert concatenate_ids(pd.np.nan) is None



