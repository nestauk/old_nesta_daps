from nesta.packages.mag.query_mag_api import get_journal_articles 
from nesta.packages.mag.parse_abstract import uninvert_abstract

ARXIV_MAG = {'id':'DOI',
             'datestamp': 'D',
             'created': 'D',
             'updated': 'D',
             'title': 'DN',
             'doi':'DOI',
             'abstract': 'IA',
             'authors' : 'AA',
             'citation_count': 'CC'}

def get_biorxiv_articles(api_key, start_date='1 Jan, 2000'):
    """Get all biorxiv articles from the MAG API.
    
    Args:
        api_key (str): MAG API key
        start_date (str): Sensibly formatted date string (interpretted by pd)
    Yields:
        article (dict): article object ready for insertion via nesta's arxiv ORM
    """
    for article in get_journal_articles('biorxiv', start_date=start_date, 
                                        api_key=api_key):        
        # Convert to arxiv format for insertion to database
        article= {arxiv_field: article[mag_field]
                  for arxiv_field, mag_field in ARXIV_MAG.items()}
        article['abstract'] = uninvert_abstract(article['abstract'])
        article['id'] = f"biorxiv-{article['id']}"  # just to be sure
        yield article
