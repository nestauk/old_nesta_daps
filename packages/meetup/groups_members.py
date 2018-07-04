import requests
import logging
import time
# import ratelim

# Local imports
from utils.common.db_utils import read_all_results
from utils.common.datapipeline import DataPipeline
from retrying import retry

# RATELIM_DUR = 50 * 60
# RATELIM_QUERIES = 8500


# @ratelim.patient(RATELIM_QUERIES, RATELIM_DUR)
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


def run(config):
    groups = read_all_results(config, 'input_db', 'input_table')
    # Filter groups matching target country/category
    groups = [row for row in groups
              if row['country_name'] == config['parameters']['country']]
    groups = [row for row in groups
              if row['category_id'] == int(config['parameters']['category'])]

    # Collect group info
    groups = set((row['id'], row['urlname'])
                 for row in groups)
    logging.info("Got %s distinct groups from database", len(groups))

    api_key = config["Meetup"]["api-key"]
    max_results = 200
    output = []
    for group_id, group_urlname in groups:
        member_ids = []
        offset = 0
        while True:
            params = dict(offset=offset, page=max_results,
                          key=api_key, group_id=group_id)
            _results = get_members(params)
            member_ids += _results
            if len(_results) < max_results:
                break
            offset += 1
        for member_id in set(member_ids):
            row = dict(member_id=member_id,
                       group_urlname=group_urlname,
                       group_id=group_id)
            output.append(row)

    logging.info("Got %s rows of data", len(output))
    with DataPipeline(config) as dp:
        for row in output:
            dp.insert(row)
