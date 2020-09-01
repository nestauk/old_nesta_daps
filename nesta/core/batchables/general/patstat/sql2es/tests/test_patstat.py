from unittest import


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


    PATH
def test_reformat_row():

            _titles = select_metadata(Tls202ApplnTitle, _session, appln_ids)
        _abstrs = select_metadata(Tls203ApplnAbstr, _session, appln_ids)
        ipcs = select_metadata(Tls209ApplnIpc, _session, appln_ids)
        nace2s = select_metadata(Tls229ApplnNace2, _session, appln_ids)
        techs = select_metadata(Tls230ApplnTechnField, _session, appln_ids)
        # Get persons                                                                                                             
        _pers_applns = select_metadata(Tls207PersAppln, _session, appln_ids)
        pers_ids = set(pa['person_id'] for pa in _pers_applns)
        persons = select_metadata(Tls906Person, _session, pers_ids,
                                  field_selector=Tls906Person.person_id)

    # Extract text fields                                                                                                         
    title = select_text(_titles, 'appln_title_lg', 'appln_title')
    abstr = select_text(_abstrs, 'appln_abstract_lg', 'appln_abstract')
