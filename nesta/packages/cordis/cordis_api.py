import requests
import pandas as pd
import json
from retrying import retry
from nesta.packages.decorators.ratelimit import ratelimit

TOP_PREFIX = 'http://cordis.europa.eu/{}'
CSV_URL = TOP_PREFIX.format('data/cordis-{}projects.csv')
API_URL = TOP_PREFIX.format('api/details')

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
    url = TOP_PREFIX.format('api/details')
    if api is not None:
        url = f'{API_URL}/{api}'
    r = requests.get(url, params={'lang': 'en',
                                  'rcn': rcn,
                                  'paramType': 'rcn',
                                  'contenttype': content_type})
    r.raise_for_status()
    return r.json()['payload']


def extract_fields(data, fields):
    out_data = {}
    for field in fields:        
        value = data[field]
        if type(value) is list:
            value = [_row['title'] for _row in value]
        out_data[field] = value
    return out_data


def get_ids(framework):
    df = pd.read_csv(CSV_URL.format(framework),
                     engine='c',
                     decimal=',', sep=';',
                     error_bad_lines=False,
                     warn_bad_lines=True,
                     encoding='latin')
    return list(df.rcn)


def fetch_data(rcn):
    # Collect project info
    _project = hit_api(rcn=rcn, content_type='project')
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
    reports = [extract_fields(rep, REPS_FIELDS) for rep in _reports]
    # Collect publications via OpenAIRE
    pubs = hit_api(api='openaire', rcn=rcn)
    return project, orgs, reports, pubs


if __name__ == "__main__":
    #all_rcn = set(get_ids('fp7') + get_ids('h2020'))
    #print("Processing", len(all_rcn), "projects")    
    all_rcn = {89242}
    n_proj, _orgs, n_reps, n_pubs = 0, set(), 0, 0
    for rcn in all_rcn:
        project, orgs, reports, pubs = fetch_data(rcn)
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
        
