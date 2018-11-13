'''
String matching
===============

Tools for matching strings in strings
'''


# Lightly adapted from http://code.activestate.com/recipes/117214/
def knuth_morris_pratt(text, pattern, overlaps=False):
    '''Yields all starting positions of copies of the pattern in the text.
    Calling conventions are similar to string.find, but its arguments can be
    lists or iterators, not just strings, it returns all matches, not just
    the first one, and it does not need the whole text in memory at once.
    Whenever it yields, it will have read the text exactly up to and including
    the match that caused the yield.

    Args:
        text (str): Text to be scanned. Could be any iterable, but note that
                    the type should match that of :obj:`pattern`.
        pattern (str): Pattern to match in text.
        overlaps (bool): Allow overlapping matches.
    '''

    # allow indexing into pattern and protect against change during yield
    pattern = list(pattern)

    # build table of shift amounts
    shifts = [1] * (len(pattern) + 1)
    shift = 1
    for pos in range(len(pattern)):
        while shift <= pos and pattern[pos] != pattern[pos-shift]:
            shift += shifts[pos-shift]
        shifts[pos+1] = shift

    # do the actual search
    startPos = 0
    matchLen = 0
    lastMatch = -len(pattern)
    for c in text:
        while matchLen == len(pattern) or \
              matchLen >= 0 and pattern[matchLen] != c:
            startPos += shifts[matchLen]
            matchLen -= shifts[matchLen]
        matchLen += 1
        if matchLen == len(pattern):
            if overlaps or startPos >= lastMatch + len(pattern):
                yield startPos
                lastMatch = startPos
