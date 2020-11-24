from unittest import mock

from nesta.packages.nih.preprocess_nih import get_json_cols
from nesta.packages.nih.preprocess_nih import get_long_text_cols
from nesta.packages.nih.preprocess_nih import is_nih_null
from nesta.packages.nih.preprocess_nih import expand_prefix_list
from nesta.packages.nih.preprocess_nih import remove_generic_suffixes
from nesta.packages.nih.preprocess_nih import remove_large_spaces
from nesta.packages.nih.preprocess_nih import replace_question_with_best_guess
from nesta.packages.nih.preprocess_nih import remove_trailing_exclamation
from nesta.packages.nih.preprocess_nih import clean_text
from nesta.packages.nih.preprocess_nih import detect_and_split
from nesta.packages.nih.preprocess_nih import preprocess_row

from nesta.packages.nih.preprocess_nih import pd
from nesta.core.orms.nih_orm import Projects, Abstracts, Publications
PATH = 'nesta.packages.nih.preprocess_nih.{}'


def test_get_json_cols():
    assert get_json_cols(Projects) == {'funding_ics', 'org_duns', 'pi_ids',
                                       'pi_names', 'project_terms',
                                       'nih_spending_cats'}
    assert get_json_cols(Publications) == {'author_list'}
    assert get_json_cols(Abstracts) == set()


def test_get_long_text_cols():
    assert get_long_text_cols(Projects) == {'cfda_code', 'core_project_num',
                                            'base_core_project_num',
                                            'ed_inst_type', 'foa_number',
                                            'full_project_num',
                                            'funding_mechanism', 'ic_name',
                                            'org_city', 'org_country',
                                            'org_dept', 'org_name',
                                            'org_zipcode', 'phr',
                                            'program_officer_name',
                                            'project_title',
                                            'study_section_name'}
    assert get_long_text_cols(Publications) == {'affiliation', 'author_name',
                                                'country', 'journal_issue',
                                                'journal_title',
                                                'journal_title_abbr',
                                                'journal_volume',
                                                'page_number', 'pub_title'}
    assert get_long_text_cols(Abstracts) == {'abstract_text'}


def test_is_nih_null():
    for value in ('', [], {}, 'N/A', 'Not Required', 'None', None, pd.np.nan):
        assert is_nih_null(value)
    for value in (0, 1, False, True, list, bool, 'something', ' ', 
                  '\t', -1, -123.4, 234.5, set()):
        assert not is_nih_null(value)
    

@mock.patch(PATH.format('GENERIC_PREFIXES'), ['this is a prefix', 'this too'])
def test_expand_prefix_list():
    assert all(term in expand_prefix_list()
               for term in ['this is a prefix - ', 'This Is A Prefix - ',
                            '?THIS IS A PREFIX ', '?This Is A Prefix ',
                            'This Is A Prefix/ ', 'this is a prefix-',
                            'This Too - ', '?THIS TOO ', 'this too: ',
                            'this too-', '4 - ', '0: ', '?6 ', '6 -', '5/ '])
    assert len(expand_prefix_list()) == 224


@mock.patch(PATH.format('GENERIC_PREFIXES'), ['this is a prefix', 'this too'])
def test_remove_generic_suffixes():
    before = '5/0:this too/?THIS IS A PREFIX this is the actual text!'
    after = 'this is the actual text!'
    assert remove_generic_suffixes(before) == after


def test_remove_large_spaces():
    before = '   some\tbasic\t\ttest  text \t\there '
    after = 'some basic test text here'
    assert remove_large_spaces(before) == after


def test_replace_question_with_best_guess():
    before = ("These are the question?s questions ? not the ?quotation,? "
              "and ?such.? Don't you think? Not that I shouldn?t ask "
              "?True?False? questions like that?")
    after = ("These are the question's questions - not the 'quotation', "
             "and 'such'. Don't you think? Not that I shouldn't ask "
             "'True - False' questions like that?")
    assert replace_question_with_best_guess(before) == after


def test_remove_trailing_exclamation():
    before = "this isn't loud!!"
    after = "this isn't loud"
    assert remove_trailing_exclamation(before) == after


@mock.patch(PATH.format('GENERIC_PREFIXES'), ['this is a prefix', 'this too'])
def test_clean_text():
    before = ("5/0:this too/?THIS IS A PREFIX These are the "
              "question?s questions ? not the ?quotation,? "
              "and ?such.? Don't you think? Not that I shouldn?t ask "
              "?True?False? questions like that? !  ")
    after = ("These are the question's questions - not the 'quotation', "
             "and 'such'. Don't you think? Not that I shouldn't ask "
             "'True - False' questions like that?")    
    assert clean_text(before) == after
    
    before = "THIS SHOULD BE LOWERCASE ? DON?T YOU THINK?"
    after = "This Should Be Lowercase - Don't You Think?"
    assert clean_text(before) == after


def test_detect_and_split():
    # More ; than ,
    assert detect_and_split('split;me;up, please!') == ['split', 'me', 
                                                        'up, please!']
    # One more , than ;
    assert detect_and_split('split,me;up, please!') == ['split,me',
                                                        'up, please!']
    # Two or more , than ;
    assert detect_and_split('split,me;up, please,!') == ['split', 'me;up',
                                                         ' please', '!']
    # Equal numbers of , and ;
    assert detect_and_split('split;me;up, please,!') == ['split', 'me', 
                                                         'up, please,!']
    # No ; or n
    assert detect_and_split('split me up please!') == ['split me up please!']
    

def test_preprocess_row_text():
    row_before = {'abstract_text': ("5/0:this too/?THIS IS A PREFIX These "
                                    "are the question?s questions ? not "
                                    "the ?quotation,? and ?such.? Don't "
                                    "you think? Not that I shouldn?t ask "
                                    "?True?False? questions like that? !  "),
                  "application_id": "DON?T CHANGE ME!"}
    row_after = {'abstract_text': ("These are the question's questions - "
                                   "not the 'quotation', and 'such'. Don't "
                                   "you think? Not that I shouldn't ask "
                                   "'True - False' questions like that?"),
                 "application_id": "DON?T CHANGE ME!"}
    assert preprocess_row(row_before, Abstracts) == row_after


def test_preprocess_row_json_colons():
    row_before = {'author_list': 'FOO, BAR; DOE, JANE',
                  'pmid': 234}
    row_after = {'author_list': ['Foo, Bar','Doe, Jane'],
                 'pmid': 234}
    assert preprocess_row(row_before, Publications) == row_after


def test_preprocess_row_json_commas():
    row_before = {'author_list': 'FOO BAR, DOE; JANE, DOE, JOHN',
                  'pmid': 234}
    row_after = {'author_list': ['Foo Bar','Doe; Jane', 'Doe', 'John'],
                 'pmid': 234}
    assert preprocess_row(row_before, Publications) == row_after

