import requests
import logging
import time
from retrying import retry
from . import meetup_utils


@retry(wait_random_min=200, wait_random_max=10000, stop_max_attempt_number=10)
def get_members(params):
    '''Hit the Meetup API for the members of a specified group.
    
    Args:
        params (:obj:`dict`): :code:`https://api.meetup.com/members/` parameters
        
    Returns:
        (:obj:`list` of :obj:`str`): Meetup member IDs
    '''
    params['key'] = meetup_utils.get_api_key()
    # Set the offset parameter and make the request
    r = requests.get("https://api.meetup.com/members/", params=params)
    r.raise_for_status()
    # If no response is found
    if len(r.text) == 0:
        time.sleep(5)
        return get_members(params)
    data = r.json()
    return [row['id'] for row in data['results']]


def get_all_members(group_id, group_urlname, max_results, test=False):
    '''Get all of the Meetup members for a specified group.
    
    Args:
        group_id (int): The Meetup ID of the group.
        group_urlname (str): The URL name of the group.
        max_results (int): The maximum number of results to return per API query.
        test (bool): For testing.
        
    Returns:
        (:obj:`list` of :obj:`dict`): A matchable list of Meetup members
    '''
    member_ids = []
    offset = 0
    while True:
        params = dict(offset=offset, page=max_results,
                      group_id=group_id)
        _results = get_members(params)
        member_ids += _results
        if len(_results) < max_results:
            break
        offset += 1
        if test:
            break

    # Join together unique member ids
    members = []
    for member_id in set(member_ids):
        row = dict(member_id=member_id,
                   group_urlname=group_urlname,
                   group_id=group_id)
        members.append(row)
    return members


if __name__ == "__main__":
    import json
    logging.getLogger().setLevel(logging.INFO)
    
    # Get input data from somewhere
    with open("data/country_groups.json", "r") as f:
        groups = json.load(f)

    # Collect group info
    groups = set((row['id'], row['urlname']) for row in groups)
    logging.info("Got %s distinct groups from database", len(groups))

    # Collect members
    output = []
    for group_id, group_urlname in groups:
        logging.info("Getting %s", group_urlname)
        members = get_all_members(group_id, group_urlname, max_results=200)
        output += members
    logging.info("Got %s members", len(output))

    # Write the output
    meetup_utils.save_sample(output, 'data/groups_members.json', 20)
