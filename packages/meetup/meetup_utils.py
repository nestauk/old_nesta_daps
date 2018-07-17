import os
import random

def get_api_key():
    api_keys = os.environ["MEETUP_API_KEYS"].split(",")
    return random.choice(api_keys)
