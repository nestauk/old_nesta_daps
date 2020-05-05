from nesta.packages.mag.query_mag_api import get_journal_articles 
from nesta.packages.mag.parse_abstract import uninvert_abstract

"""
Schema transformation of arxiv ORM from MAG raw data,
so that 'xiv data from MAG can slot into arxiv pipelines.
"""
ARXIV_MAG = {'id':'DOI',
             'datestamp': 'D',
             'created': 'D',
             'updated': 'D',
             'title': 'DN',
             'doi':'DOI',
             'abstract': 'IA',
             'authors' : 'AA',
             'citation_count': 'CC'}


def get_magrxiv_articles(xiv, api_key, start_date='1 Jan, 2000'):
    """Get all specific "xiv" articles from the MAG API.
    
    Args:
        xiv (str): One of 'biorxiv' or 'medrxiv' (but would work for non-xiv journal titles,
                   but viability depends on volume of articles, so approach would likely
                   fail for e.g. PMC)
        api_key (str): MAG API key
        start_date (str): Sensibly formatted date string (interpretted by pd)
    Yields:
        article (dict): article object ready for insertion via nesta's arxiv ORM
    """
    for article in get_journal_articles(xiv, start_date=start_date, 
                                        api_key=api_key):  
        # Convert to arxiv format for insertion to database
        article= {arxiv_field: article[mag_field]
                  for arxiv_field, mag_field in ARXIV_MAG.items()
                  if mag_field in article}
        if 'abstract' in article:
            article['abstract'] = uninvert_abstract(article['abstract'])
        else:
            article['abstract'] = None
        article['id'] = f"{xiv}-{article['id']}"  # just to be sure
        article['article_source'] = xiv
        yield article
