import pytest
from unittest import mock

from nesta.packages.mag.query_mag_api import prepare_title
from nesta.packages.mag.query_mag_api import build_expr
from nesta.packages.mag.query_mag_api import query_mag_api
from nesta.packages.mag.query_mag_api import dedupe_entities
from nesta.packages.mag.query_mag_sparql import extract_entity_id
from nesta.packages.mag.query_mag_sparql import query_articles_by_doi


class TestPrepareTitle:
    def test_prepare_title_removes_extra_spaces(self):
        assert prepare_title("extra   spaces       here") == "extra spaces here"
        assert prepare_title("trailing space ") == "trailing space"

    def test_prepare_title_replaces_invalid_characters_with_single_space(self):
        assert prepare_title("invalid%&*^˙∆¬stuff") == "invalid stuff"
        assert prepare_title("ba!!!!d3") == "ba d3"

    def test_prepare_title_lowercases(self):
        assert prepare_title("UPPER") == "upper"

    def test_prepare_title_handles_empty_titles(self):
        assert prepare_title(None) == ""


class TestBuildExpr:
    def test_build_expr_correctly_forms_query(self):
        assert list(build_expr([1, 2], 'Id', 1000)) == ["expr=OR(Id=1,Id=2)"]
        assert list(build_expr(['cat', 'dog'], 'Ti', 1000)) == ["expr=OR(Ti='cat',Ti='dog')"]

    def test_build_expr_respects_query_limit_and_returns_remainder(self):
        assert list(build_expr([1, 2, 3], 'Id', 21)) == ["expr=OR(Id=1,Id=2)", "expr=OR(Id=3)"]


@mock.patch('nesta.packages.mag.query_mag_api.requests.post', autospec=True)
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


def test_dedupe_entities_picks_highest_for_each_title():
    entities = [{'Id': 1, 'Ti': 'test title', 'logprob': 44},
                {'Id': 2, 'Ti': 'test title', 'logprob': 10},
                {'Id': 3, 'Ti': 'another title', 'logprob': 5},
                {'Id': 4, 'Ti': 'another title', 'logprob': 10}]

    assert dedupe_entities(entities) == {1, 4}


class TestExtractEntity:
    def test_extract_entity_id_returns_integer_id(self):
        assert extract_entity_id('http://ma-graph.org/entity/109214941') == 109214941
        assert extract_entity_id('http://ma-graph.org/entity/19694890') == 19694890
        assert extract_entity_id('http://ma-graph.org/entity/13203339') == 13203339

    def test_extract_entity_id_returns_string_id(self):
        assert extract_entity_id('http://ma-graph.org/entity/test_id') == 'test_id'
        assert extract_entity_id('http://ma-graph.org/entity/another_id') == 'another_id'
        assert extract_entity_id('http://ma-graph.org/entity/grid.011.5') == 'grid.011.5'

    def test_extract_entity_id_raises_value_error_when_not_found(self):
        with pytest.raises(ValueError):
            extract_entity_id('bad_url')


class TestQueryArticles:
    @mock.patch('nesta.packages.mag.query_mag_sparql._batch_query_articles_by_doi', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.levenshtein_distance', autospec=True)
    def test_query_articles_by_doi_returns_closest_match(self,
                                                         mocked_lev_distance,
                                                         mocked_batch):
        missing_articles = [{'id': 1, 'doi': '1.1/1234', 'title': 'title_a'},
                            {'id': 2, 'doi': '2.2/4321', 'title': 'title_b'}]

        results_batch = [{'paperTitle': 'title_aaa', 'doi': '1.1/1234'},
                         {'paperTitle': 'title_aa', 'doi': '1.1/1234'},
                         {'paperTitle': 'title_b', 'doi': '2.2/4321'},
                         {'paperTitle': 'title_c', 'doi': '2.2/4321'}]

        mocked_batch.return_value = [(missing_articles, results_batch)]
        mocked_lev_distance.side_effect = [2, 1, 0, 1]

        result = list(query_articles_by_doi(missing_articles))
        assert result == [{'paperTitle': 'title_aa', 'score': 1, 'id': 1, 'doi': '1.1/1234'},
                          {'paperTitle': 'title_b', 'score': 0, 'id': 2, 'doi': '2.2/4321'}]


    @mock.patch('nesta.packages.mag.query_mag_sparql._batch_query_articles_by_doi', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.levenshtein_distance', autospec=True)
    def test_query_articles_by_doi_returns_no_results_when_doi_not_found(self,
                                                                         mocked_lev_distance,
                                                                         mocked_batch):
        missing_articles = [{'id': 1, 'doi': '1.1/1234', 'title': 'title_a'},
                            {'id': 2, 'doi': 'bad_doi', 'title': 'title_b'}]

        results_batch = [{'paperTitle': 'title_aaa', 'doi': '1.1/1234'},
                         {'paperTitle': 'title_aa', 'doi': '1.1/1234'}]

        mocked_batch.return_value = [(missing_articles, results_batch)]
        mocked_lev_distance.side_effect = [2, 1]

        result = list(query_articles_by_doi(missing_articles))
        assert result == [{'paperTitle': 'title_aa', 'score': 1, 'id': 1, 'doi': '1.1/1234'}]
