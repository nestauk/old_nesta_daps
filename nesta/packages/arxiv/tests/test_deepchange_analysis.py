from alchemy_mock.comparison import ExpressionMatcher
from datetime import date
from io import BytesIO
import matplotlib.pyplot as plt
import pandas as pd
import sqlalchemy
from unittest import mock
import pytest

# for mocking
from nesta.packages.arxiv.deepchange_analysis import ArticleTopic, CorExTopic
from nesta.packages.arxiv.deepchange_analysis import boto3

# to be tested
from nesta.packages.arxiv.deepchange_analysis import add_before_date_flag
from nesta.packages.arxiv.deepchange_analysis import calculate_rca_by_country
from nesta.packages.arxiv.deepchange_analysis import get_article_ids_by_term
from nesta.packages.arxiv.deepchange_analysis import get_article_ids_by_terms
from nesta.packages.arxiv.deepchange_analysis import plot_to_s3
from nesta.packages.arxiv.deepchange_analysis import highly_cited
from nesta.packages.arxiv.deepchange_analysis import is_multinational

PREFIX = 'nesta.packages.arxiv.deepchange_analysis.{}'

def test_add_before_date_flag_correctly_adds_flags():
    data = pd.DataFrame({'id': [0, 1, 2],
                         'date': [date(2010, 1, 1),
                                  date(2009, 12, 31),
                                  date(2012, 5, 8)]})

    expected_result = pd.DataFrame({'id': [0, 1, 2],
                                    'before_2010': [False, True, False]})

    result = add_before_date_flag(data, date_column='date', before_year=2010)

    pd.testing.assert_frame_equal(result[['id', 'before_2010']], expected_result)


def test_calculate_rca_by_country_calculates_correct_results():
    data = pd.DataFrame({'country': ['USA', 'USA', 'USA', 'China', 'China', 'China'],
                         'data_to_ignore': ['one', 'two', 'three', 'four', 'five', 'six'],
                         'flag': [True, False, False, True, True, False]})

    expected_result = pd.DataFrame({'country': ['China', 'USA'],
                                    'flag': [1.333, 0.667]}).set_index('country')

    result = calculate_rca_by_country(data,
                                      country_column='country',
                                      commodity_column='flag')

    pd.testing.assert_frame_equal(expected_result, result,
                                  check_exact=False, check_less_precise=3)


@mock.patch.object(boto3, 'resource', autospec=True)
def test_plot_to_s3_sends_chart_to_s3(mocked_boto3_resource):
    mocked_obj = mock.Mock()
    mocked_s3 = mock.Mock()
    mocked_s3.Object.return_value = mocked_obj
    mocked_boto3_resource.return_value = mocked_s3

    empty_plot = plt.figure()
    stream = BytesIO()
    empty_plot.savefig(stream, format='png', bbox_inches='tight')

    plot_to_s3(bucket='test_bucket',
               filename='my_file',
               plot=empty_plot,
               image_format='png')

    sent_to_s3 = mocked_obj.put.call_args[1]['Body']  # extract the object sent to s3
    assert mocked_boto3_resource.call_args == mock.call('s3')
    assert sent_to_s3.getvalue() == stream.getvalue()
    assert mocked_s3.Object.call_args == mock.call('test_bucket', 'my_file')


@mock.patch.object(boto3, 'resource', autospec=True)
def test_plot_to_s3_applies_public_permissions(mocked_boto3_resource):
    mocked_obj = mock.Mock()
    mocked_s3 = mock.Mock()
    mocked_s3.Object.return_value = mocked_obj
    mocked_boto3_resource.return_value = mocked_s3

    empty_plot = plt.figure()

    plot_to_s3(bucket='test_bucket',
               filename='my_file',
               plot=empty_plot,
               public=True)

    assert mocked_s3.Object().put.call_args[1]['ACL'] == 'public-read'

    plot_to_s3(bucket='test_bucket',
               filename='my_file',
               plot=empty_plot,
               public=False)

    assert 'ACL' not in mocked_s3.Object().put.call_args[1].keys()


@mock.patch(PREFIX.format("db_session"), autospec=True)
def test_get_article_ids_by_term(mocked_db_session):
    mocked_session = mock.Mock(spec=sqlalchemy.orm.session)
    mocked_engine = mock.Mock(spec=sqlalchemy.engine)
    mocked_db_session(mocked_engine).__enter__.return_value = mocked_session

    articles = [ArticleTopic(article_id=5),
                ArticleTopic(article_id=6),
                ArticleTopic(article_id=7)]

    mocked_session.query().filter().scalar.return_value = 1
    mocked_session.query().filter().all.return_value = articles
    mocked_session.reset_mock()  # gets rid of the extra calls while setting up the mock

    result = get_article_ids_by_term(mocked_engine, term='cats', min_weight=0.1)
    expected_result = {5, 6, 7}
    assert result == expected_result

    # ExpressionMatcher required when comparing sqlalchemy binary operators
    assert (ExpressionMatcher(mocked_session.query().filter.mock_calls[0])
            == mock.call(sqlalchemy.func.json_contains(CorExTopic.terms, '["cats"]')))

    assert (ExpressionMatcher(mocked_session.query().filter.mock_calls[2])
            == mock.call((ArticleTopic.topic_id == 1) & (ArticleTopic.topic_weight >= 0.1)))


@mock.patch(PREFIX.format("db_session"), autospec=True)
@mock.patch(PREFIX.format("get_article_ids_by_term"))
def test_get_article_ids_by_term(mocked_get_ids, mocked_db_session):
    mocked_session = mock.Mock(spec=sqlalchemy.orm.session)
    mocked_engine = mock.Mock(spec=sqlalchemy.engine)
    mocked_db_session(mocked_engine).__enter__.return_value = mocked_session
    mocked_get_ids.side_effect = [{1,2,3},
                                  {},
                                  {2,4,5}]
    ids = get_article_ids_by_terms(mocked_engine, terms=['a','b','c'],
                                   min_weight=0.3)
    assert ids == {1,2,3,4,5}


@mock.patch(PREFIX.format("db_session"), autospec=True)
@mock.patch(PREFIX.format("get_article_ids_by_term"))
def test_get_article_ids_by_term_fail(mocked_get_ids, mocked_db_session):
    mocked_session = mock.Mock(spec=sqlalchemy.orm.session)
    mocked_engine = mock.Mock(spec=sqlalchemy.engine)
    mocked_db_session(mocked_engine).__enter__.return_value = mocked_session
    mocked_get_ids.side_effect = [{}, {}, {}]
    with pytest.raises(ValueError):
        get_article_ids_by_terms(mocked_engine, terms=['a','b','c'],
                                 min_weight=0.3)


def test_highly_cited():
    lookup = pd.DataFrame({'citation_count': [2, 8, 7]}, index=[2000, 2001, 2002])

    row = pd.Series({'id': 1, 'citation_count': 5, 'year': 2000})
    assert highly_cited(row, lookup) is True

    row = pd.Series({'id': 2, 'citation_count': 6, 'year': 2001})
    assert highly_cited(row, lookup) is False

    row = pd.Series({'id': 3, 'citation_count': 7, 'year': 2002})
    assert highly_cited(row, lookup) is False


def test_is_multinational():
    countries = ['China', 'Joel', 'Nesta']
    # Should be True
    for text in ['Something (China)', '(Something) (China)',
                 'blasj.erer  ereren ererer (Joel)',
                 'erere (ere) (Nesta)']:
        assert is_multinational(text, countries)

    # Should be False
    for text in ['(China)', '(China) (Something)',
                 'blasj.erer  ereren ererer',
                 'erere (Nesta) (Nesta']:
        assert not is_multinational(text, countries)
