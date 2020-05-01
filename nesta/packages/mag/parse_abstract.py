from string import punctuation as PUNC
PUNC = set(PUNC) - set([')',']'])

def _uninvert_abstract(inverted_abstract):
    terms = [None]*inverted_abstract['IndexLength']
    for term, idxs in inverted_abstract['InvertedIndex'].items():
        for idx in idxs:
            terms[idx] = term
    if None in terms:
        raise ValueError('Inverted abstract is incomplete')
    return terms


def should_insert_full_stop(first_term, second_term):
    # e.g. 'Background:' unlikely to be end of sentence
    # however 'background' could be
    first_ends_with_punc = first_term[-1] in PUNC

    # e.g. 'Background Radiation' unlikely to be two sentences
    # however 'background Radiation' could be 
    first_is_capital = first_term[0].isupper()
    second_is_capital = second_term[0].isupper()
    
    # e.g. 'background IR' unlikely to be two sentences
    second_is_acro = second_term.isupper()
    # Accept e.g. 'ABCs' as an acronym
    if second_term.endswith('s'):
        second_is_acro = second_term[:-1].isupper()

    # These two requirements are conflicting: acronym rule takes precedent
    second_is_ok = second_is_capital and not second_is_acro

    return not any((first_ends_with_punc, first_is_capital,
                    not second_is_ok))


def insert_full_stops(terms):
    for idx in range(len(terms[:-1])):
        first_term = terms[idx]
        second_term = terms[idx+1]
        if should_insert_full_stop(first_term, second_term):
            terms[idx] += '.'
    return terms

    
def uninvert_abstract(inverted_abstract):
    terms = _uninvert_abstract(inverted_abstract)
    terms = insert_full_stops(terms)
    return ' '.join(terms)

# {id:'DOI',
#  'datestamp': 'D',
#  'created': 'D',
#  'updated': 'D'
#  'title': 'DN',
#  'doi':'DOI',
#  'abstract': ''
# authors
# mag_authors
# mag_id
# mag_match_prob
# citation_count
# citation_count_updated
# msc_class
# institute_match_attempted
