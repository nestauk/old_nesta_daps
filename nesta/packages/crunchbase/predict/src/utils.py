def split_str(text):
    """Split a string on comma."""
    return text.split(',')

def flatten_lists(lst):
    """Remove nested lists. """
    return [item for sublist in lst for item in sublist]
