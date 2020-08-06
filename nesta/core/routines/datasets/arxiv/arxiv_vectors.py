"""
arXiv vectors
==============

Tasks for converting arXiv abstracts to vectors via BERT in batches.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.misctools import f3p
from nesta.core.luigihacks.misctools import load_batch_config
from nesta.core.luigihacks.text2vectask import Text2VecTask
from nesta.core.orms.arxiv_orm import Article, ArticleVector

import luigi
from datetime import datetime as dt


class ArxivVectorTask(luigi.WrapperTask):
    process_batch_size = luigi.IntParameter(default=5000)
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())

    def requires(self):
        set_log_level(not self.production)
        batch_kwargs = load_batch_config(self)
        return Text2VecTask(id_field=Article.id,
                            text_field=Article.abstract,
                            batchable=f3p('batchables/nlp/bert_vectorize'),
                            process_batch_size=self.process_batch_size,
                            in_class=Article,
                            out_class=ArticleVector,
                            **batch_kwargs)

    def run(self):
        self.output().touch()
