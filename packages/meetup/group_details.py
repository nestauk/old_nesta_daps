import logging
import requests
import time
from retrying import retry
from meetup import meetup_utils


class NoGroupFound(Exception):
    '''Exception should no group be found by the Meetup API'''
    def __init__(self, group_urlname):
        super().__init__("No such group found: {}".format(group_urlname))


@retry(wait_random_min=200, wait_random_max=10000, stop_max_attempt_number=10)
def get_group_details(group_urlname, max_results):
    '''Hit the Meetup API for the details of a specified groups.
    Args:
        group_urlname (str): A Meetup group urlname
        max_results (int): Total number of results to return per API request.
    Returns: 
        (:obj:`list` of :obj:`dict`): Meetup API response data
    '''    
    params = dict(sign='true', fields='topics', page=max_results)
    params['key'] = meetup_utils.get_api_key()
    r = requests.get('https://api.meetup.com/{}'.format(group_urlname),
                     params=params)
    group_info = r.json()
    if 'errors' in group_info:
        raise NoGroupFound(group_urlname)
    return group_info
    

if __name__ == "__main__":
    import json
    logging.getLogger().setLevel(logging.INFO)

    # Load all groups that have been found
    groups_members = []
    for dsname in ['members_groups', 'groups_members']:
        with open('data/{}.json'.format(dsname), 'r') as f:
            groups_members += json.load(f)

    # Get each group details
    _output = []
    for row in groups_members:
        logging.info(row['group_urlname'])
        _info = get_group_details(row['group_urlname'], max_results=200)
        _output.append(_info)

    # Flatten the output    
    output = meetup_utils.flatten_data(_output,
                                       keys = [('category', 'name'), 
                                               ('category', 'shortname'),
                                               ('category', 'id'),
                                               'created',
                                               'country',
                                               'city',
                                               'description',
                                               'id',
                                               'lat',
                                               'lon',
                                               'members',
                                               'name', 
                                               'topics',
                                               'urlname'])
        
    # Write the output
    meetup_utils.save_sample(output, 'data/group_details.json', 20)
