import os
import random
import json
import numpy as np
from nesta.production.orms.orm_utils import db_session
from nesta.production.orms.meetup_orm import Group
from collections import Counter


def get_api_key():
    '''Get a random API key from those listed in the environmental variable
    :code:`MEETUP_API_KEYS`.
    '''
    api_keys = os.environ["MEETUP_API_KEYS"].split(",")
    return random.choice(api_keys)

def save_sample(json_data, filename, k):
    '''Dump a sample of :code:`k` items from row-oriented JSON
    data :code:`json_data` into file with name :code:`filename`.
    '''
    with open(filename, 'w') as fp:
        json.dump(random.sample(json_data, k), fp)


def flatten_data(list_json_data, keys, **kwargs):
    '''Flatten nested JSON data from a list of JSON objects, by
    a list of desired keys. Each element in the :code:`keys`
    may also be an ordered iterable of keys,
    such that subsequent keys describe a path through the
    JSON to desired value. For example in order to extract
    `key1` and `key3` from:

    .. code-block:: python

        {'key': <some_value>, 'key2' : {'key3': <some_value>}}

    one would specify :code:`keys` as:

    .. code-block:: python

        ['key1', ('key2', 'key3')]

    Args:
        list_json_data (:obj:`json`): Row-orientated JSON data.
        keys (:obj:`list`): Mixed list of either: individual `str` keys for data values
        which are not nested; **or** sublists of `str`, as described above.
        **kwargs: Any constants to include in every flattened row of the output.

    Returns:
       :obj:`json`: Flattened row-orientated JSON data.
    '''
    # Loop through groups
    output = []
    for info in list_json_data:
        row = dict(**kwargs)
        # Generate the field names and values, if they exist
        for k in keys:
            field_name = k
            try:
                # If the key is just a string
                if type(k) == str:
                    value = info[k]
                # Otherwise, assume its a list of keys
                else:
                    field_name = "_".join(k)
                    # Recursively assign the list of keys
                    value = info
                    for _k in k:
                        value = value[_k]
            # Ignore fields which aren't found (these will appear
            # as NULL in the database anyway)
            except KeyError:
                continue
            row[field_name] = value
        output.append(row)
    return output


def get_members_by_percentile(engine, perc=10):
    """Get the number of meetup group members for a given percentile
    from the database.
    
    Args:
        engine: A SQL alchemy connectable.
        perc (int): A percentile to evaluate.
    Returns:
        members (float): The number of members corresponding to this percentile.
    """
    with db_session(engine) as session:
        rows = (session
                .query(Group.members)
                .all())
        rows = [r[0] for r in rows]
    return float(np.percentile(rows, perc))


def get_core_topics(engine, core_categories, members_limit, perc=99):
    """Get the most frequent topics from a selection of meetup categories,
    from the database.
    
    Args:
        engine: A SQL alchemy connectable.
        core_categories (list): A list of category_shortnames.
        members_limit (int): Minimum number of members required in a group 
                             for it to be considered.
        perc (int): A percentile to evaluate the most frequent topics.
    Returns:
        topics (set): The set of most frequent topics.
    """
    with db_session(engine) as session:
        rows = (session
                .query(Group.topics)
                .filter(Group.members >= members_limit)
                .filter(Group.category_shortname.in_(core_categories))
                .all())
        rows = [r[0] for r in rows]
    topic_counts = Counter(t['name'] for topics in rows for t in topics)
    topic_cutoff = np.percentile(list(float(v) for v in topic_counts.values()), perc)
    return set(k for k, v in topic_counts.items() if v >= topic_cutoff)
