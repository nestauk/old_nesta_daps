from unittest import mock
from nesta.core.batchables.general.patstat.sql2es.run import select_text
from nesta.core.batchables.general.patstat.sql2es.run import reformat_row

PATH = 'nesta.core.batchables.general.patstat.sql2es.run.{}'


def test_select_text_english():
    longest_english = 'this is some longer text'
    objs = [{'lang': 'en', 'text': 'this is some text'},
            {'lang': 'fr', 'text': 'cette texte est la plus longue'},
            {'lang': 'en', 'text': longest_english}]
    text = select_text(objs, 'lang', 'text')
    assert text == longest_english


def test_select_text_no_english():
    longest = 'cette texte est la plus longue'
    objs = [{'lang': 'it', 'text': 'questo e alcuno testo'},
            {'lang': 'fr', 'text': longest},
            {'lang': 'es', 'text': 'este es alguno texto'}]
    text = select_text(objs, 'lang', 'text')
    assert text == longest


def test_select_text_none():
    assert select_text([], 'lang', 'text') is None


@mock.patch(PATH.format('db_session'))
@mock.patch(PATH.format('select_metadata'))
def test_reformat_row(mocked_select_meta, mocked_session):
                                      # Titles
    mocked_select_meta.side_effect = [[{'appln_title': 'title one', 'appln_title_lg': 'en'},
                                       {'appln_title': 'titilo due', 'appln_title_lg': 'it'}],
                                      # Abstracts
                                      [{'appln_abstract': 'abstr one', 'appln_abstract_lg': 'en'},
                                       {'appln_abstract': 'abstr due', 'appln_abstract_lg': 'it'}],
                                      # IPC codes
                                      [{'ipc_class_symbol': 'A 123'}, {'ipc_class_symbol': 'A 234'},
                                       {'ipc_class_symbol': 'B 234'}, {'ipc_class_symbol': 'X 234'}],
                                      # NACE2 codes
                                      [{'nace2_code': 'N1'}, {'nace2_code': 'N2'},
                                       {'nace2_code': 'Nx'}, {'nace2_code': 'Nx'}],
                                      # Techn field codes
                                      [{'techn_field_nr': 'T1'}, {'techn_field_nr': 'Tx'},
                                       {'techn_field_nr': 'T1'}, {'techn_field_nr': 'T3'}],
                                      # Dummy person ids
                                      [],
                                      # Persons
                                      [{'person_ctry_code': 'GB', 'nuts': 'GB123'},
                                       {'person_ctry_code': 'IT', 'nuts': 'IT123'}]]

    mocked_session.return_value.__enter__.return_value = None
    row = {'appln_id': [], 'some field': 'some value', 'another field': 'another value'}

    expected_output = {'title': 'title one',
                       'abstract': 'abstr one',
                       'ipc': ['A', 'B', 'X'],
                       'nace2': ['N1', 'N2', 'Nx'],
                       'tech': ['T1', 'T3', 'Tx'],
                       'ctry': ['GB', 'IT'],
                       'nuts': ['GB123', 'IT123'],
                       'is_eu': True,
                       'some field': 'some value',
                       'another field': 'another value'}
    assert reformat_row(row, None) == expected_output
