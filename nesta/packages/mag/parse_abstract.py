from string import punctuation as PUNC
PUNC = set(PUNC) - set([')',']'])

def _uninvert_abstract(inverted_abstract):
    """Create a list of terms from a MAG inverted abstract field
    
    Args:
        inverted_abstract (dict): The "IA" field from a MAG article
    Returns:
        terms (list): A list of terms created from the inverted abstract
    """
    terms = [None]*inverted_abstract['IndexLength']
    for term, idxs in inverted_abstract['InvertedIndex'].items():
        for idx in idxs:
            terms[idx] = term
    terms = list(filter((None).__ne__, terms))  # Filter missing terms
    return terms


def is_acronym(term):
    """Returns True only if the term is all uppercase or 
    the term is plural (ends with an 's') and is otherwise uppercase.

    Args:
        term (str): Term to test
    Returns:
        bool
    """
    return term.isupper() or (term.endswith('s') and term[:-1].isupper())


def should_insert_full_stop(first_term, second_term):
    """Returns False (i.e. a full-stop should NOT be inserted
    between terms) if ANY of the following conditions are met:
    a) the first term ends with punctuation,
    b) the first term is capitalised (acronyms do not count)
    c) the second term is not capitalised (acronyms do not count).
    
    Args:
        {first, second}_term (str): The first and second terms, between which
                                    a full-stop could be placed.
    Returns:
        bool
    """
    # e.g. 'Background:' unlikely to be end of sentence
    # however 'background' could be
    first_ends_with_punc = first_term[-1] in PUNC

    # e.g. 'Background Radiation' unlikely to be two sentences
    # however 'background Radiation' could be
    first_is_capital = first_term[0].isupper()
    second_is_capital = second_term[0].isupper()

    # e.g. 'IR Radiation' likely to be two sentences
    # however 'IR radiation' is not two sentences
    first_is_acro = is_acronym(first_term)
    # These two requirements are conflicting: acronym rule takes precedent
    first_is_capital = first_is_capital and not first_is_acro

    # e.g. 'background IR' unlikely to be two sentences
    second_is_acro = is_acronym(second_term)
    # These two requirements are conflicting: acronym rule takes precedent
    second_is_capital = second_is_capital and not second_is_acro

    return not any((first_ends_with_punc, first_is_capital,
                    not second_is_capital))


def insert_full_stops(terms):
    """Append full-stops to terms which appear to be the end of sentences.
    
    Args:
        terms (list): An ordered list of terms, without missing full-stops.
    Returns:
        terms (list): An ordered list of terms, with imputed full-stops.
    """
    for idx in range(len(terms[:-1])):
        first_term = terms[idx]
        second_term = terms[idx+1]
        if should_insert_full_stop(first_term, second_term):
            terms[idx] += '.'
    # If final char not in PUNC, then append a full stop
    if terms[-1][-1] not in PUNC:
        terms[-1] += '.'
    return terms


def uninvert_abstract(inverted_abstract):
    """Uninvert a MAG inverted abstract, and impute missing full-stops.
    See https://docs.microsoft.com/en-us/academic-services/project-academic-knowledge/reference-paper-entity-attributes#invertedabstract
    
    Args:
        inverted_abstract (dict): The "IA" field from a MAG article.
    Returns:
        text (str): A block of abstract text, from the inverted abstract.
    """
    terms = _uninvert_abstract(inverted_abstract)
    terms = insert_full_stops(terms)
    return ' '.join(terms)
