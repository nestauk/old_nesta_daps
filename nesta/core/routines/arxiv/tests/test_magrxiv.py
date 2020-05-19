from nesta.core.routines.arxiv.magrxiv_collect_iterative_task import get_article_ids
from nesta.core.routines.arxiv.magrxiv_collect_iterative_task import get_articles
from nesta.core.routines.arxiv.magrxiv_collect_iterative_task import _get_articles
from unittest import mock
import pytest

PATH='nesta.core.routines.arxiv.magrxiv_collect_iterative_task.{}'


@pytest.fixture
def dummy_kwargs():
    return {'xiv': 'blarxiv',
            'api_key': '123',
            'start_date': '20 Apr, 2019',
            'article_ids': set()}


@mock.patch(PATH.format('Article'))
def test_get_article_ids_returns_set(Article):
    session = mock.Mock()
    session.query().filter().all.return_value = [mock.Mock()]
    ids = get_article_ids(session, [])
    assert type(ids) is set


@mock.patch(PATH.format('_get_articles'))
def test_get_articles_test_flush(mocked_get_articles, dummy_kwargs):
    xiv = dummy_kwargs.pop('xiv')
    _articles = [{'id': 1}, {'id': 2}, {'id': 4}]
    for n_xivs in range(2, 10):
        dummy_kwargs['xivs'] = [xiv]*n_xivs
        for flush_every in range(1, len(_articles)+1):
            mocked_get_articles.return_value = _articles*n_xivs
            articles = get_articles(test=True, flush_every=flush_every, **dummy_kwargs)
            assert len(articles) >= flush_every*n_xivs


@mock.patch(PATH.format('get_magrxiv_articles'))
def test__get_articles_test_flush(mocked_get_articles, dummy_kwargs):
    mocked_get_articles.return_value = [{'id': 1}, {'id': 2}, {'id': 4}, {'id': 2}, {'id': 5}]
    n_unique_ids = 4
    for flush_every in range(1, n_unique_ids+1):
        dummy_kwargs['article_ids'] = set()
        articles = _get_articles(test=True, flush_every=flush_every, **dummy_kwargs)
        assert len(dummy_kwargs['article_ids']) <= n_unique_ids
        assert len(articles) == flush_every

    for flush_every in range(n_unique_ids, n_unique_ids+10):
        dummy_kwargs['article_ids'] = set()
        articles = _get_articles(test=True, flush_every=flush_every, **dummy_kwargs)
        assert len(dummy_kwargs['article_ids']) <= n_unique_ids
        assert len(articles) == n_unique_ids
