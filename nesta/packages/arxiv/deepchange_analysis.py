import boto3
from datetime import date
from io import BytesIO
import logging
import sqlalchemy
import re

from nesta.core.orms.arxiv_orm import ArticleTopic, CorExTopic
from nesta.core.orms.orm_utils import db_session


def is_multinational(text, countries):
    """Returns True if :obj:`text` ends with any of
    :obj:`countries` in brackets. For example:

        'Blah blah (Germany)'
        'Something  (China)'
        'oe.,e.de (United States)'
    
    would return True.
    """
    results = re.findall(r"^.* \((.*)\)$", text)
    if len(results) == 0:
        return False
    is_mn = (results[-1] in countries)
    return is_mn


def add_before_date_flag(data, date_column, before_year):
    """Consumes a dataframe and appends a boolean column 'before_x' where x is the
    cutoff date, containing True if the specified date field is before this year.

    Args:
        data (:code:`pandas.DataFrame`): df containing a date column
        date_column (str): label of the column containg the date
        before_year (int): year to use for the cutoff

    Returns
        (:code:`pandas.DataFrame`): df with the new column appended
    """
    data[f'before_{before_year}'] = data[date_column] < date(before_year, 1, 1)

    return data


def calculate_rca_by_country(data, country_column, commodity_column):
    """Groups a dataframe by country and calculates the Revealed Comparative Advantage
    (RCA) for each country, based on a boolean commodity column.

    Args:
        data (:code:`pandas.DataFrame`): df containing a date column
        country_column (str): label of the column containing countries
        commodity_column (str): label of the boolean column showing if the row
            represents the desired commodity, eg is_dl

    Returns:
        (:code:`pandas.DataFrame`): grouped dataframe with country and calculated RCA
    """
    world_export_proportion = data[commodity_column].sum() / data[commodity_column].count()

    country_groups = data[[country_column, commodity_column]].groupby(country_column)
    rca = (country_groups.sum() / country_groups.count()) / world_export_proportion

    return rca


def plot_to_s3(bucket, filename, plot, image_format='png', pad_x=False, public=True):
    """Takes a matplotlib plot, exports it as an image and sends it to an S3 bucket.

    Args:
        bucket (str): name of the S3 bucket
        plot (:code:`matplotlib.pyplot`): plot to export
        filename (str): name of the generated file on S3
        image_format (str): format of the generated image
        pad_x (bool): pad the x axis by half a tick on each side
        public (bool): apply public read permissions to the images

    Returns:
        (dict): response from boto3
    """
    stream = BytesIO()

    if pad_x is True:
        x0, x1 = plot.xlim()
        plot.xlim(x0 - 0.5, x1 + 0.5)
    plot.savefig(stream, format=image_format, bbox_inches="tight")
    stream.seek(0)
    logging.info(f"Exporting {filename} to {bucket}")

    permissions = {}
    if public is True:
        permissions = {'ACL': 'public-read'}

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, filename)
    return obj.put(Body=stream, **permissions)


def get_article_ids_by_term(engine, term, min_weight):
    """Identifies articles related to a term.

    The topic id is collected from sql and then a list of article ids is returned.

    Args:
        engine (:code:`sqlalchemy.engine`): connection to the database
        term (str): term to search for
        min_weight (float): minimum acceptable weight for matches

    Returns:
        (set): ids of articles which are in that topic, at or above the specified weight
    """
    with db_session(engine) as session:
        topic_id = (session
                    .query(CorExTopic.id)
                    .filter(sqlalchemy.func.json_contains(CorExTopic.terms,
                                                          f'["{term}"]'))
                    .scalar())
        if topic_id is None:
            raise ValueError(f'{term} not found in any topics')
        logging.info(f"Identified {term} topic with id {topic_id}")

        articles = (session
                    .query(ArticleTopic.article_id)
                    .filter((ArticleTopic.topic_id == topic_id)
                            & (ArticleTopic.topic_weight >= min_weight))
                    .all())

    article_ids = {a.article_id for a in articles}
    logging.info(f"Identified {len(article_ids)} deep learning articles in database")

    return article_ids


def get_article_ids_by_terms(engine, terms, min_weight):
    """Identifies articles related multiple terms.

    The topic id is collected from sql and then a 
    list of article ids is returned.

    Args:
        engine (:code:`sqlalchemy.engine`): connection to the database
        terms (list): List of terms to search for
        min_weight (float): minimum acceptable weight for matches

    Returns:
        (set): ids of articles which are in any of the topic, at or above the specified weight
    """
    dl_topic_ids = set()
    for term in terms:
        try:
            _ids = get_article_ids_by_term(engine=engine,
                                           term=term,
                                           min_weight=min_weight)
        except ValueError:
            logging.info(f'{term} NOT found in topics')
        else:
            logging.info(f'{term} found in topics')
            dl_topic_ids = dl_topic_ids.union(_ids)
    if len(dl_topic_ids) == 0:
        raise ValueError(f'None of {terms} found in any topic')
    return dl_topic_ids


def highly_cited(row, lookup):
    """Determines if an article has more citations than the yearly median.

    Args:
        row(:code:`pandas.Series`): a single article
        lookup(:code:`pandas.DataFrame`): table of years and median citation counts

    Returns:
        (bool): True if greater than the yearly median
    """
    return (row.citation_count > lookup.loc[row.year]).bool()
