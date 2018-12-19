

def chunks(l, n):
    """ chunks
    Yield successive n-sized chunks from l.
    
    Args:
        l (iter): an iterable
        n (int): size of chunks to yield
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]
