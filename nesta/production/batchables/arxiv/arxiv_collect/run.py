import boto3
from contextlib import contextmanager
import logging
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from urllib.parse import urlsplit

from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data
from nesta.production.orms.arxiv_orm import Base, Articles, ArticleCategories, Categories
from nesta.packages.arxiv.collect_arxiv import request_token, arxiv_batch, load_arxiv_categories


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def retrieve_rows(start_cursor, end_cursor, resumption_token):
    '''Iterate through batches and yield single rows until the end_cursor or end of data
    is reached.

    Args:
        start_cursor (int): first record to return
        end_cursor (int): start of the next batch, ie stop when this cursor is returned
        resumption_token (int): token to supply the api

    Returns:
        (dict): a single row of data
    '''
    while start_cursor is not None and start_cursor < end_cursor:
        batch, start_cursor = arxiv_batch(resumption_token, start_cursor)
        for row in batch:
            yield row


@contextmanager
def db_session(engine):
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def run():
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    start_cursor = int(os.environ["BATCHPAR_start_cursor"])
    end_cursor = int(os.environ["BATCHPAR_end_cursor"])

    logging.warning(f"Retrieving articles between {start_cursor}:{end_cursor - 1}")

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
    for row in retrieve_rows(start_cursor, end_cursor, resumption_token):
        with db_session(engine) as session:
            categories = row.pop('categories', [])
            articles.append(row)
            for cat in categories:
                try:
                    session.query(Categories).filter(Categories.id == cat).one()
                except NoResultFound:
                    logging.warning(f"missing category: '{cat}' for article {row['id']}.  Adding to Categories table")
                    session.add(Categories(id=cat))
                article_cats.append(dict(article_id=row['id'], category_id=cat))

    inserted_articles, existing_articles = insert_data("BATCHPAR_config", "mysqldb", db_name,
                                                       Base, Articles, articles,
                                                       return_existing=True)
    inserted_article_cats, existing_article_cats = insert_data("BATCHPAR_config", "mysqldb", db_name,
                                                               Base, ArticleCategories, article_cats,
                                                               return_existing=True)

    # sanity checks before the batch is marked as done
    if len(inserted_articles) + len(existing_articles) != len(articles):
        raise ValueError(f'Inserted articles do not match original data')

    if len(inserted_article_cats) + len(existing_article_cats) != len(articles):
        raise ValueError(f'Inserted article categories do not match original data')

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        # level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
