"""
run.py (nlp.bert_vectorize)
===========================

Vectorize text documents via BERT
"""

from ast import literal_eval
import boto3
import json
import logging
import os

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.orm_utils import get_class_by_tablename
from nesta.core.orms.orm_utils import get_base_from_orm_name

from sentence_transformers import SentenceTransformer

def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    os.environ["MYSQLDB"] = os.environ["BATCHPAR_config"]
    bert_model_name = os.environ["BATCHPAR_bert_model"]
    
    # Instantiate SentenceTransformer
    model = SentenceTransformer(bert_model)

    # Database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # Retrieve list of Org ids from S3
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    _ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(_ids)} objects retrieved from s3")

    _class = get_class_by_tablename(in_module, in_tablename)
    id_attribute = getattr(_class, id_field)
    text_attribute = getattr(_class, text_field)
    with db_session(engine) as session:
        query = (session
                 .query(id_attribute, text_attribute)
                 .filter(id_attribute in _ids))
        rows = [object_to_dict(row) for row in query.all()]
    ids = [row[id_field] for row in rows]
    docs = [row[text_field] for row in rows]
    del rows

    # Convert text to vectors
    embeddings = model.encode(docs)
    out_data = [{"article_id": _id, "vector": embedding}
                for _id, embedding in zip(ids, embeddings)]
    Base = get_base_from_orm_name(out_module)
    out_class = get_class_by_tablename(out_module, out_tablename)
    insert_data("BATCHPAR_config", "mysqldb", db_name, Base,
                out_class, out_data, low_memory=True)
    

if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
