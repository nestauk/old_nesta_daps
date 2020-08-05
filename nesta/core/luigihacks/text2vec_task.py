"""
Text to vectors, via BERT
=========================

Tasks for converting documents in MySQL to vectors via BERT, in batches.
"""


from nesta.core.luigihacks.sql2batchtask import Sql2BatchTask
from nesta.core.luigihacks.misctools import f3p
from nesta.core.luigihacks.parameter import SqlAlchemyParameter
import inspect
import os
import luigi


def get_class_info(_class):
    _path, _file = os.path.split(inspect.getfile(Article))
    _, _path = os.path.split(_path)
    pathstub = os.path.join(_path, _file)
    tablename = _class.__tablename__
    return pathstub, tablename


class Text2VectorTask(Sql2BatchTask):
    batchable = luigi.Parameter(f3p('batchables/nlp/bert_vectorize'))
    in_class = SqlAlchemyParameter()
    out_class = SqlAlchemyParameter()

    def __init__(self, *args, **kwargs):
        for arg_name in ('in_class', 'out_class'):
            try:
                _class = kwargs.pop(arg_name)
            except KeyError:
                raise AttributeError(f'{arg_name} must be a named argument.')
            pathstub, tablename = get_class_info(_class)
            kwargs['kwargs'][f'{arg_name}_pathstub'] = pathstub
            kwargs['kwargs'][f'{arg_name}_tablename'] = tablename
        super().__init__(*args, **kwargs)
