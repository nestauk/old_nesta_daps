import boto3
import logging
import os
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlsplit

from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import try_until_allowed
from nesta.production.orms.arxiv_orm import Base, Article, ArticleCategories
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


def run():
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    start_cursor = int(os.environ["BATCHPAR_start_cursor"])
    end_cursor = int(os.environ["BATCHPAR_end_cursor"])

    logging.warning(f"Retrieving articles between {start_cursor}:{end_cursor - 1}")

    # Setup the database connectors
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)

    # load arxiv subject categories to database
    bucket = 'innovation-mapping-general'
    cat_file = 'arxiv_classification/arxiv_subject_classifications.csv'
    load_arxiv_categories("BATCHPAR_config", db_name, bucket, cat_file)

    # process data
    resumption_token = request_token()
    for row in retrieve_rows(start_cursor, end_cursor, resumption_token):
        try:
            categories = row.pop('categories')
        except KeyError:
            pass
        else:
            for cat in categories:
                session.add(ArticleCategories(article_id=row['id'], category_id=cat))

        session.add(Article(**row))

    session.commit()
    session.close()

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
