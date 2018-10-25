import logging
import requests
import time
from retrying import retry
from . import meetup_utils


class NoMemberFound(Exception):
    '''Exception should no member be found by the Meetup API'''
    def __init__(self, member_id):
        super().__init__("No such member found: {}".format(member_id))


@retry(wait_random_min=200, wait_random_max=10000, stop_max_attempt_number=10)
def get_member_details(member_id, max_results):
    '''Hit the Meetup API for details of a specified member

    Args:
        member_id (str): A Meetup member ID
        max_results (int): The maximum number of results with each API hit

    Returns:
        :obj:`list` of :obj:`dict`: Meetup API response json.
    '''
    params = dict(sign='true', fields='memberships', page=max_results)
    params['key'] = meetup_utils.get_api_key()
    r = requests.get('https://api.meetup.com/members/{}'.format(member_id),
                     params=params)
    member_info = r.json()    
    if 'errors' in member_info:
        raise NoMemberFound(member_id)
    return member_info


def get_member_groups(member_info):
    '''Extract the groups data from Meetup membership information.
    
    Args:
        member_id (str): A Meetup member ID
        member_info (:obj:`list` of :obj:`dict`): Meetup member API response json.
    
    Returns:
        :obj:`list` of :obj:`dict`: List of unique member-group combinations
    '''
    output = []
    # If memberships aren't in the data the ignore this member
    member_id=int(member_info['id'])
    row = dict(member_id=member_id)
    if 'memberships' not in member_info:
        logging.warning('No info for {}'.format(member_id))
        output.append(row)
        return output

    # Group information exists for both member and organizer entities
    for key in ['member', 'organizer']:
        if key not in member_info['memberships']:
            continue
        for membership in member_info['memberships'][key]:
            row["group_id"] = membership['group']['id']
            row["group_urlname"] = membership['group']['urlname']
            output.append(row.copy())
    return output


if __name__ == "__main__":
    import json
    logging.getLogger().setLevel(logging.INFO)

    with open("data/groups_members.json", "r") as f:
        groups_members = json.load(f)
    
    # Generate the groups for these members
    output = []
    for row in groups_members:
        member_info = get_member_details(row['member_id'], max_results=200)
        output += get_member_groups(member_info)
                  
    # Write the output
    meetup_utils.save_sample(output, 'data/members_groups.json', 20)
