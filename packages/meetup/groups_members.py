import requests
import logging
import time
from retrying import retry
import json
import os


@retry(wait_random_min=2000, wait_random_max=60000, stop_max_attempt_number=10)
def get_members(params):
    # Set the offset parameter and make the request
    r = requests.get("https://api.meetup.com/members/", params=params)
    r.raise_for_status()
    # If no response is found
    if len(r.text) == 0:
        time.sleep(5)
        return get_members(params)
    data = r.json()
    return [row['id'] for row in data['results']]


def get_all_members(group_id, max_results):
    member_ids = []
    offset = 0
    while True:
        params = dict(offset=offset, page=max_results,
                      key=os.environ["MEETUP_API_KEY"], group_id=group_id)
        _results = get_members(params)
        member_ids += _results
        if len(_results) < max_results:
            break
        offset += 1

    # Join together unique member ids
    members = []
    for member_id in set(member_ids):
        row = dict(member_id=member_id,
                   group_urlname=group_urlname,
                   group_id=group_id)
        members.append(row)
    return members


if __name__ == "__main__":
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
        members = get_all_members(group_id, max_results=200)
        output += members
    logging.info("Got %s members", len(output))
