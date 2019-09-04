"""
Cordis API
==========

Extract all Cordis data via the API, by project.
"""

import requests
import pandas as pd
import json
from retrying import retry
from nesta.packages.decorators.ratelimit import ratelimit
from nesta.packages.misc_utils.camel_to_snake import camel_to_snake
from json.decoder import JSONDecodeError
from requests.exceptions import HTTPError

TOP_PREFIX = 'http://cordis.europa.eu/{}'
CSV_URL = TOP_PREFIX.format('data/cordis-{}projects.csv')

INFO_FIELDS = ['rcn', 'acronym', 'startDateCode',
               'endDateCode', 'framework',
               'fundedUnder', 'status', 'title',
               'ecContribution', 'totalCost', 'website']
OBJS_FIELDS = ['fundingScheme', 'objective', 'projectDescription',
               'topics', 'proposalCall']
REPS_FIELDS = ['rcn', 'finalResults', 'workPerformed',
               'teaser', 'summary', 'title']
ORGS_FIELDS = ['activityType', 'address', 'contribution',
               'country', 'name', 'organizationId',
               'type', 'website']


@retry(stop_max_attempt_number=10)
@ratelimit(max_per_second=10)
def hit_api(api='', rcn=None, content_type=None):
    """
    Hit the Cordis API by project code

    Args:
        api (str): Assumed to support '' (cordis) or 'openaire'.
        rcn (str): RCN id of the project or entity to find.
        content_type (str): contenttype argument for Cordis API
    Returns:
        data (json)
    """
    url = TOP_PREFIX.format('api/details')
    if api is not None:
        url = f'{url}/{api}'
    r = requests.get(url, params={'lang': 'en',
                                  'rcn': rcn,
                                  'paramType': 'rcn',
                                  'contenttype': content_type})
    if (r.status_code == 500 and 
        r.json()['payload']['errorType'] == 'ica'):
        return None
    r.raise_for_status()
    return r.json()['payload']


def extract_fields(data, fields):
    """
    Extract specific fields and flatten data from Cordis API.

    Args:
        data (dict): A row of data to be processed.
        fields (list): A list of fields to be extracted.
    Returns:
        out_data (dict): Flatter data, with specific fields extracted.
    """
    out_data = {}
    for field in fields:
        if field not in data:
            continue
        value = data[field]
        if type(value) is list:
            value = [{k: _row[k] for k in ['title', 'rcn']}
                     for _row in value]
        snake_field = camel_to_snake(field)
        out_data[snake_field] = value
    return out_data


def get_framework_ids(framework, nrows=None):
    """
    Get all IDs of projects by funding framework.

    Args:
        framework (str): 'fp7' or 'h2020'
    Returns:
        ids (list)
    """
    df = pd.read_csv(CSV_URL.format(framework),
                     nrows=nrows,
                     engine='c',
                     decimal=',', sep=';',
                     error_bad_lines=False,
                     warn_bad_lines=True,
                     encoding='latin')
    return list(df.rcn)


def filter_pubs(pubs):
    """Remove publications without links, and merge
    datasets and publications data together. 
    Also deduplicates publications based on pids.
    
    Args:
        pubs (dict): Publication data from OpenAIRE.
    Returns:
        _pubs (list): Flattened list of input data.
    """
    _pubs, pids = [], set()
    for p in pubs['datasets'] + pubs['publications']:
        if 'pid' not in p:
            continue
        already_found = any(id in pids for id in p['pid'])
        pids = pids.union(p['pid'])
        if already_found or len(p['pid']) == 0:
            continue
        _pubs.append(dict(id=p['pid'][0], **p))
    return _pubs


def fetch_data(rcn):
    """
    Fetch all data (project, reports, orgs, publications)
    for a given project id.

    Args:
        rcn (str): Project id.
    Returns:
        data (tuple): project, orgs, reports, pubs
    """
    # Collect project info
    _project = hit_api(rcn=rcn, content_type='project')
    if _project is None:
        return (None,None,None,None)
    info = _project['information']    
    project = {**extract_fields(info, INFO_FIELDS),
               **extract_fields(_project['objective'],
                                OBJS_FIELDS)}
    # Collect organisations
    orgs = []
    for _orgs in _project['organizations'].values():
        orgs += [extract_fields(org, ORGS_FIELDS)
                 for org in _orgs
                 if 'organizationId' in org]
    # Collect result reports
    _reports = [hit_api(rcn=report['rcn'], content_type='result')
                for report in info['relatedResultsReport']]
    reports = []
    if _reports is not None:
        reports = [extract_fields(rep, REPS_FIELDS)
                   for rep in _reports]
    # Collect publications via OpenAIRE
    try:
        pubs = hit_api(api='openaire', rcn=rcn)
        if pubs is None:
            raise HTTPError
    except (HTTPError, JSONDecodeError):
        pubs = []
    else:
        pubs = filter_pubs(pubs)
    return project, orgs, reports, pubs


if __name__ == "__main__":
    #all_rcn = set(get_framework_ids('fp7') +/
    #              get_framework_ids('h2020'))
    #print("Processing", len(all_rcn), "projects")    
    all_rcn = {89242}
    n_proj, _orgs, n_reps, n_pubs = 0, set(), 0, 0
    for rcn in all_rcn:
        project, orgs, reports, pubs = fetch_data(rcn)
        if project is None:
            continue
        n_proj += 1
        _orgs = _orgs.union(row['organizationId'] for row in orgs)
        n_reps += len(reports)
        n_pubs += len(pubs)
        # Split orgs into permanent + link table
        # Insert everything, with project id for reference
    print(n_proj, n_reps, n_pubs, len(_orgs))
    #print(pubs['publications'])
    #print(pubs['datasets'])

    print(project)
    # for p in pubs['publications']:
    #     doi = None
    #     for pid in p['pid']:
    #         doi = pid
    #         if 'doi' in pid:
    #             break
    #     if doi is None:
    #         continue
