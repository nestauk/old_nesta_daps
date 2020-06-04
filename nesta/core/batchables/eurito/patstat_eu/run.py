"""
run.py (patstat_eu)
-------------------

Transfer pre-collected PATSTAT data from MySQL
to Elasticsearch. Only EU patents since the year 2000 are considered.
The patents are grouped by patent families.
"""

from ast import literal_eval
import boto3
import json
import logging
import os

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.patstat_eu_orm import ApplnFamily
from nesta.core.orms.patstat_2019_05_13 import *
from nesta.packages.geo_utils.lookup import get_eu_countries


def select_text(objs, lang_field, text_field):
    if len(objs) == 0:
        return None
    _objs = [t for t in objs if t[lang_field] == 'en']
    if len(_objs) == 0:
        _objs = objs
    obj = sorted(_objs, key=lambda x: len(x), reverse=True)[0]
    return obj[text_field]


def metadata(orm, session, appln_ids, field_selector=None):
    if field_selector is None:
        field_selector = orm.appln_id
    _filter = field_selector.in_(appln_ids)
    return [object_to_dict(_obj) for _obj in
            session.query(orm).filter(_filter).all()]


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']

    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # database setup
    logging.info('Retrieving engine connection')
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb",
                              db_name)
    _engine = get_mysql_engine("BATCHPAR_config", "readonly",
                               "patstat_2019_05_13")

    # es setup
    logging.info('Connecting to ES')
    strans_kwargs={'filename':'patstat.json',
                   'from_key':'tier_0', 'to_key':'tier_1',
                   'ignore':['id']}
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in
                                      os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           auto_translate=True,
                           auto_translate_kwargs={'min_len':20},
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           do_sort=True,
                           ngram_fields=['textBody_abstract_patent'])

    # collect file
    logging.info('Retrieving patent family ids')
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    docdb_fam_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(docdb_fam_ids)} patent family IDs "
                 "retrieved from s3")

    eu_countries = get_eu_countries()

    logging.info('Processing rows')
    _filter = ApplnFamily.docdb_family_id.in_(docdb_fam_ids)
    with db_session(engine) as session:
        for obj in session.query(ApplnFamily).filter(_filter).all():
            row = object_to_dict(obj)
            appln_ids = row.pop('appln_id')
            with db_session(_engine) as _session:
                _titles = metadata(Tls202ApplnTitle, _session, appln_ids)
                _abstrs = metadata(Tls203ApplnAbstr, _session, appln_ids)
                ipcs = metadata(Tls209ApplnIpc, _session, appln_ids)
                nace2s = metadata(Tls229ApplnNace2, _session, appln_ids)
                techs = metadata(Tls230ApplnTechnField, _session, appln_ids)
                # Get persons
                _pers_applns = metadata(Tls207PersAppln, _session, appln_ids)
                pers_ids = set(pa['person_id'] for pa in _pers_applns)
                persons = metadata(Tls906Person, _session, pers_ids,
                                   field_selector=Tls906Person.person_id)

            title = select_text(_titles, 'appln_title_lg', 'appln_title')
            abstr = select_text(_abstrs, 'appln_abstract_lg', 'appln_abstract')

            # Get names from lookups
            ipcs = list(set(i['ipc_class_symbol'].split()[0] for i in ipcs))
            nace2s = list(set(n['nace2_code'] for n in nace2s))
            techs = list(set(t['techn_field_nr'] for t in techs))
            ctrys = list(set(p['person_ctry_code'] for p in persons))
            nuts = list(set(p['nuts'] for p in persons))
            is_eu = any(c in eu_countries for c in ctrys)

            # Index the data
            row = dict(title=title, abstract=abstr, ipc=ipcs, nace2=nace2s,
                       tech=techs, ctry=ctrys, nuts=nuts, is_eu=is_eu, **row)
            uid = row.pop('docdb_family_id')
            _row = es.index(index=es_index, doc_type=es_type,
                            id=uid, body=row)


    logging.warning("Batch job complete.")


if __name__ == "__main__":
    set_log_level()
    logging.info('Starting...')
    run()
