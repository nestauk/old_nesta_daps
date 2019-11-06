import boto3
import logging
import os
from sqlalchemy.orm.exc import NoResultFound
from urllib.parse import urlsplit

from nesta.core.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data, db_session
from nesta.core.orms.arxiv_orm import Base, Article, ArticleCategory, Category
from nesta.packages.arxiv.collect_arxiv import request_token, load_arxiv_categories, retrieve_arxiv_batch_rows
from nesta.core.luigihacks.s3 import parse_s3_path


def run():
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    start_cursor = int(os.environ["BATCHPAR_start_cursor"])
    end_cursor = int(os.environ["BATCHPAR_end_cursor"])
    batch_size = end_cursor - start_cursor
    logging.warning(f"Retrieving {batch_size} articles between {start_cursor - 1}:{end_cursor - 1}")

    # Setup the database connectors
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)

    # load arxiv subject categories to database
    bucket = 'innovation-mapping-general'
    cat_file = 'arxiv_classification/arxiv_subject_classifications.csv'
    load_arxiv_categories("BATCHPAR_config", db_name, bucket, cat_file)

    # process data
    articles = []
    article_cats = []
    resumption_token = request_token()
    for row in retrieve_arxiv_batch_rows(start_cursor, end_cursor, resumption_token):
        with db_session(engine) as session:
            categories = row.pop('categories', [])
            articles.append(row)
            for cat in categories:
                # TODO:this is inefficient and should be queried once to a set. see
                # iterative proceess.
                try:
                    session.query(Category).filter(Category.id == cat).one()
                except NoResultFound:
                    logging.warning(f"missing category: '{cat}' for article {row['id']}.  Adding to Category table")
                    session.add(Category(id=cat))
                article_cats.append(dict(article_id=row['id'], category_id=cat))

    inserted_articles, existing_articles, failed_articles = insert_data(
                                                "BATCHPAR_config", "mysqldb", db_name,
                                                Base, Article, articles,
                                                return_non_inserted=True)
    logging.warning(f"total article categories: {len(article_cats)}")
    inserted_article_cats, existing_article_cats, failed_article_cats = insert_data(
                                                "BATCHPAR_config", "mysqldb", db_name,
                                                Base, ArticleCategory, article_cats,
                                                return_non_inserted=True)

    # sanity checks before the batch is marked as done
    logging.warning((f'inserted articles: {len(inserted_articles)} ',
                     f'existing articles: {len(existing_articles)} ',
                     f'failed articles: {len(failed_articles)}'))
    logging.warning((f'inserted article categories: {len(inserted_article_cats)} ',
                     f'existing article categories: {len(existing_article_cats)} ',
                     f'failed article categories: {len(failed_article_cats)}'))
    if len(inserted_articles) + len(existing_articles) + len(failed_articles) != batch_size:
        raise ValueError(f'Inserted articles do not match original data.')
    if len(inserted_article_cats) + len(existing_article_cats) + len(failed_article_cats) != len(article_cats):
        raise ValueError(f'Inserted article categories do not match original data.')

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
