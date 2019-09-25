import itertools


def flatten_lists(l):
    """Unpacks nested lists into one list of elements."""
    return list(itertools.chain(*l))


def dicts2sql_format(d1, d2):
    """Combine keys and values from two dictionaries to format them
    as rows to be stored in SQL.

    Args:
        d1 (:obj:`dict`): Dictionary containing IDs that will be used as Primary Key.
        d2 (:obj:`dict`): Dictionary containing the cluster number and probability.

    Return:
        (:obj:`list`): List containing the rows of a SQL table.

    """
    return [[(id_, k, v) for k, v in arr.items()] for id_, arr in zip(d1.keys(), d2)]
