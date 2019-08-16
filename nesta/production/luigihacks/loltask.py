from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.packages.novelty.lolvelty import lolvelty
from nesta.production.orms.orm_utils import setup_es
from nesta.production.orms.orm_utils import get_es_ids
import luigi

class LolTask(luigi.Task):
    test = luigi.BoolParameter(default=True)
    dataset = luigi.Parameter()
    fields = luigi.ListParameter()
    index = luigi.Parameter(default=None)
    score_field = luigi.Parameter()

    date = luigi.DateParameter()
    db_config_path = luigi.Parameter(default="mysqldb.config")

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, 
                                         "mysqldb")
        db_config["database"] = ("production" if not self.test 
                                 else "dev")
        db_config["table"] = "NIH dummy"  # Note, not a real table    
        update_id = "NihNoveltyTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        min_should_match = 0.3 if not self.test else 0.05

        es_mode = 'dev' if self.test else 'prod'
        es, es_config = setup_es(es_mode, self.test,
                                 drop_and_recreate=False,
                                 dataset=self.dataset)
        es_config['index'] = (es_config['index'] 
                              if self.index is None
                              else self.index)
        r = es.count(index=es_config['index'], 
                    body={"query": {"match_all": {}}})
        total = r['count']
        for doc_id in get_es_ids(es, es_config):
            # Check whether the doc exists with the fields
            existing = es.get(es_config['index'],
                              doc_type=es_config['type'],
                              id=doc_id)['_source']
            if not any(f in existing for f in self.fields):
                continue
            # Get the score
            score = lolvelty(es, es_config['index'], 
                             doc_id, self.fields, 
                             minimum_should_match=min_should_match,
                             total=total)
            if score is None:
                continue

            # Merge existing info into new doc
            doc = {self.score_field: score, **existing}
            es.index(index=es_config['index'], 
                     doc_type=es_config['type'], 
                     id=doc_id, body=doc)
        self.output().touch()
