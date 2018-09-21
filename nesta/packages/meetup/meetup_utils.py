import os
import random
import json

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
