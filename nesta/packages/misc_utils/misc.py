

def chunker(l, n):
    """ chunks
    Yield successive n-sized chunks from l.
    
    Args:
        l (iter): an iterable
        n (int): size of chunks to yield
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]

def grouper(g, n, fill_value=None):
    """ grouper
    Yield successive n-sized groups from g.

    Args:
        g (generator or iter): a generator or iterable
        n (int): size of groups to yield
        fill_value: object to fill any hanging space in the last group

    Returns:
        (generator): generates groups of g with length n
    """
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)
