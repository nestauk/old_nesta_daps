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
    _, _file = os.path.split(inspect.getfile(_class))
    module = _file.replace(".py", "")
    tablename = _class.__tablename__
    return module, tablename


def assert_kwarg(kwargs, arg_name):
    try:
        value = kwargs[arg_name]
    except KeyError:
        raise AttributeError(f'{arg_name} must be a named argument.')


def assert_and_pop_kwarg(kwargs, arg_name):
    assert_kwarg(kwargs, arg_name)
    return kwargs.pop(arg_name)


class Text2VectorTask(Sql2BatchTask):
    batchable = luigi.Parameter(f3p('batchables/nlp/bert_vectorize'))
    in_class = SqlAlchemyParameter()
    out_class = SqlAlchemyParameter()
    text_field = SqlAlchemyParameter()

    def __init__(self, *args, **kwargs):
        for arg_name in ('in_class', 'out_class'):
            _class = assert_and_pop_kwarg(kwargs, arg_name)
            module, tablename = get_class_info(_class)
            kwargs['kwargs'][f'{arg_name}_module'] = module
            kwargs['kwargs'][f'{arg_name}_tablename'] = tablename
        for arg_name in ('id_field', 'text_field'):
            kwargs['kwargs'][f'{arg_name}_name'] = assert_kwarg(kwargs, arg_name).key
        super().__init__(*args, **kwargs)
