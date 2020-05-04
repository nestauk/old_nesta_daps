import pytest
from unittest import mock

from nesta.packages.mag.query_mag_api import prepare_title
from nesta.packages.mag.query_mag_api import build_expr
from nesta.packages.mag.query_mag_api import query_mag_api
from nesta.packages.mag.query_mag_api import dedupe_entities
from nesta.packages.mag.query_mag_api import get_journal_articles
from nesta.packages.mag.query_mag_api import build_composite_expr
from nesta.packages.mag.query_mag_sparql import extract_entity_id
from nesta.packages.mag.query_mag_sparql import query_articles_by_doi
from nesta.packages.mag.query_mag_sparql import _batch_query_sparql
from nesta.packages.mag.query_mag_sparql import _batched_entity_filter
from nesta.packages.mag.query_mag_sparql import MAG_ENDPOINT
from nesta.packages.mag.query_mag_sparql import get_eu_countries


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


class TestBatchQuerySparql:
    @mock.patch('nesta.packages.mag.query_mag_sparql._batched_entity_filter', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.sparql_query', autospec=True)
    def test_batch_query_sparql_raises_if_batch_size_invalid(self,
                                                             mocked_sparql_query,
                                                             mocked_entity_filter):
        batch_size_error = "batch_size must be between 1 and 50"

        with pytest.raises(ValueError) as e:
            list(_batch_query_sparql('test_query', batch_size=51))
        assert str(e.value) == batch_size_error
        mocked_sparql_query.assert_not_called()
        mocked_entity_filter.assert_not_called()

        with pytest.raises(ValueError) as e:
            list(_batch_query_sparql('test_query', batch_size=0))
        assert str(e.value) == batch_size_error
        mocked_sparql_query.assert_not_called()
        mocked_entity_filter.assert_not_called()

    @mock.patch('nesta.packages.mag.query_mag_sparql._batched_entity_filter', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.sparql_query', autospec=True)
    def test_batch_query_sparql_raises_if_wrong_arguments_provided(self,
                                                                   mocked_sparql_query,
                                                                   mocked_entity_filter):
        argument_error = ("concat_format, filter_on and ids must all "
                          "be supplied together or not at all")

        with pytest.raises(ValueError) as e:
            list(_batch_query_sparql('test_query', concat_format='concat_format'))
        assert str(e.value) == argument_error
        mocked_sparql_query.assert_not_called()
        mocked_entity_filter.assert_not_called()

        with pytest.raises(ValueError) as e:
            list(_batch_query_sparql('test_query', filter_on='filter'))
        assert str(e.value) == argument_error
        mocked_sparql_query.assert_not_called()
        mocked_entity_filter.assert_not_called()

        with pytest.raises(ValueError) as e:
            list(_batch_query_sparql('test_query', ids=['id1', 'id2']))
        assert str(e.value) == argument_error
        mocked_sparql_query.assert_not_called()
        mocked_entity_filter.assert_not_called()

    @mock.patch('nesta.packages.mag.query_mag_sparql._batched_entity_filter', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.sparql_query', autospec=True)
    def test_batch_query_sparql_queries_all_if_no_filtering_provided(self,
                                                                     mocked_sparql_query,
                                                                     mocked_entity_filter):
        mocked_sparql_query.return_value = iter([[1, 2, 3], [4, 5, 6], [7]])
        query = "SELECT ?somefield WHERE ?data graph:node ?somefield {}"

        result = list(_batch_query_sparql(query))

        assert result == [1, 2, 3, 4, 5, 6, 7]
        mocked_sparql_query.assert_called_once_with(
            MAG_ENDPOINT,
            "SELECT ?somefield WHERE ?data graph:node ?somefield "
        )
        mocked_entity_filter.assert_not_called()

    @mock.patch('nesta.packages.mag.query_mag_sparql._batched_entity_filter', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.sparql_query', autospec=True)
    def test_batch_query_sparql_limits_query_if_filtering_provided(self,
                                                                   mocked_sparql_query,
                                                                   mocked_entity_filter):
        mocked_sparql_query.return_value = iter(['a', 'b', 'c'])
        mocked_entity_filter.return_value = iter(["FILTER (?data IN (<1>,<2>,<3>))"])
        query = "SELECT ?somefield WHERE ?data graph:node ?somefield {}"
        concat_format = '<{}>'
        filter_on = 'data'
        ids = [1, 2, 3]
        batch_size = 50

        result = list(_batch_query_sparql(query, concat_format, filter_on, ids,
                                          batch_size))

        assert result == ['a', 'b', 'c']
        mocked_entity_filter.assert_called_once_with(concat_format, filter_on, ids,
                                                     batch_size)
        mocked_sparql_query.assert_called_once_with(
            MAG_ENDPOINT, ("SELECT ?somefield WHERE ?data graph:node "
                           "?somefield FILTER (?data IN (<1>,<2>,<3>))"))

    @mock.patch('nesta.packages.mag.query_mag_sparql._batched_entity_filter', autospec=True)
    @mock.patch('nesta.packages.mag.query_mag_sparql.sparql_query', autospec=True)
    def test_batch_query_sparql_splits_batches_of_ids(self,
                                                      mocked_sparql_query,
                                                      mocked_entity_filter):
        mocked_sparql_query.side_effect = iter([['a', 'b'], ['c']])
        mocked_entity_filter.return_value = iter(["FILTER (?data IN (<1>,<2>))",
                                                 "FILTER (?data IN (<3>))"])
        query = "SELECT ?somefield WHERE ?data graph:node ?somefield {}"
        concat_format = '<{}>'
        filter_on = 'data'
        ids = [1, 2, 3]
        batch_size = 2

        result = list(_batch_query_sparql(query, concat_format, filter_on, ids,
                                          batch_size))

        assert result == ['a', 'b', 'c']
        assert mocked_sparql_query.mock_calls == [
            mock.call(MAG_ENDPOINT, ("SELECT ?somefield WHERE ?data graph:node "
                                     "?somefield FILTER (?data IN (<1>,<2>))")),
            mock.call(MAG_ENDPOINT, ("SELECT ?somefield WHERE ?data graph:node "
                                     "?somefield FILTER (?data IN (<3>))"))]
        mocked_entity_filter.assert_called_once_with(concat_format, filter_on, ids,
                                                     batch_size)


class TestBatchedEntityFilter:
    @mock.patch('nesta.packages.mag.query_mag_sparql.split_batches', autospec=True)
    def test_batched_entity_filter_constructs_correct_format(self,
                                                             mocked_split_batches):
        concat_format = '<{}>'
        filter_on = 'data'
        ids = [1, 2, 3]
        batch_size = 50
        mocked_split_batches.return_value = iter([[1, 2, 3]])

        result = list(_batched_entity_filter(concat_format, filter_on, ids,
                                             batch_size))

        assert result == ["FILTER (?data IN (<1>,<2>,<3>))"]
        mocked_split_batches.assert_called_once_with(ids, batch_size)

    @mock.patch('nesta.packages.mag.query_mag_sparql.split_batches', autospec=True)
    def test_batched_entity_filter_splits_batches_of_ids(self, mocked_split_batches):
        concat_format = '<{}>'
        filter_on = 'data'
        ids = [1, 2, 3]
        batch_size = 2
        mocked_split_batches.return_value = iter([[1, 2], [3]])

        result = list(_batched_entity_filter(concat_format, filter_on, ids,
                                             batch_size))

        assert result == ["FILTER (?data IN (<1>,<2>))",
                          "FILTER (?data IN (<3>))"]
        mocked_split_batches.assert_called_once_with(ids, batch_size)


@mock.patch('nesta.packages.mag.query_mag_sparql.requests.get', autospec=True)
def test_get_eu_countries_returns_countries(mocked_requests):
    some_html = '''
        <div id="content" style="max-width:950px">
        <div class="table" id="year-entry2">
        <table><tbody><tr><th class="table_th_pos_1" colspan="2">Countries</th>
        </tr><tr><td><a href="">Austria</a></td> <td><a href="">Italy</a></td>
        </tr><tr><td><a href="">Belgium</a></td> <td><a href="">Latvia</a></td>
        </tr></tbody></table></div>
        </div>
        '''
    mocked_response = mock.Mock(text=some_html)
    mocked_requests.return_value = mocked_response

    assert get_eu_countries() == ['Austria', 'Italy', 'Belgium', 'Latvia']


def test_build_composite_expr_multiple():
    query_values = ['Journal 1', 'Journal 2']
    entity_name = 'journal name'
    date = ('Date 1', 'Date 2')
    expr_1 = "AND(Composite(journal name='Journal 1'), D=['Date 1', 'Date 2'])"
    expr_2 = "AND(Composite(journal name='Journal 2'), D=['Date 1', 'Date 2'])"
    expr = f'expr=OR({expr_1}, {expr_2})'
    assert build_composite_expr(query_values, entity_name, date) == expr


def test_build_composite_expr_single():
    query_values = ['Journal 1']
    entity_name = 'journal name'
    date = ('Date 1', 'Date 2')
    expr = "expr=AND(Composite(journal name='Journal 1'), D=['Date 1', 'Date 2'])"
    assert build_composite_expr(query_values, entity_name, date) == expr


@mock.patch('nesta.packages.mag.query_mag_api.weekchunks')
@mock.patch('nesta.packages.mag.query_mag_api.build_composite_expr')
@mock.patch('nesta.packages.mag.query_mag_api.query_mag_api')
def test_get_journal_articles(mocked_api, mocked_expr, mocked_chunks):
    n_weeks = 13
    mocked_chunks.return_value = iter([None]*n_weeks)
    # Mirror MAG behaviour
    n_article_per_chunk = 1000  # constant number of streamed articles
    n_article_second_final_chunk = 261 # any number less than n_article_per_chunk
    n_iter = 10 # so a total of 10261 articles per week
    entities = n_iter*[{'entities': [None]*n_article_per_chunk}] 
    entities += [{'entities': [None]*n_article_second_final_chunk}]
    entities += [{'entities': []}] # final chunk is empty
    n_total_per_week = n_iter*n_article_per_chunk + n_article_second_final_chunk
    mocked_api.side_effect = entities*n_weeks
    for i, article in enumerate(get_journal_articles(journal_name='journal_name', 
                                                     start_date='start_date')):
        assert i < n_total_per_week*n_weeks  # asserts no infinite loops!
    assert i == n_total_per_week*n_weeks - 1  # Final count, including one empty final iteration per week
