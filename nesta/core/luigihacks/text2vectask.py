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


def inspect_orm(_class):
    """Retrieve the python module name and the database table 
    that describe the given ORM.
    
    Args:
        _class (object): A SqlAlchemy ORM.
    Returns:
        {module, tablename} (tuple): Name of the python module and database 
                                     table that describe the given ORM.
    """
    _, _file = os.path.split(inspect.getfile(_class))
    module = _file.replace(".py", "")
    tablename = _class.__tablename__
    return module, tablename


def assert_and_retrieve_kwarg(kwargs, arg_name):
    """Retrieve an argument from kwargs.
    
    Args:
        kwargs (dict): Keyword arguments.
        arg_name (str): Name of argument which should exist.
    Returns:
        value: The value associated with the keyword argument.
    """
    try:
        value = kwargs[arg_name]
    except KeyError:
        raise AttributeError(f'{arg_name} must be a named argument.')
    return value


class Text2VecTask(Sql2BatchTask):
    """Embed any text field in MySQL as a BERT vector.
    
    Args:
        in_class (SqlAlchemyParameter): The input ORM to containing a text field to be vectorized.
        id_field (SqlAlchemyParameter): The input ORM PK field, for splitting the data into batches.
        text_field (SqlAlchemyParameter): The input ORM text field to be vectorized.
        out_class (SqlAlchemyParameter): The output ORM to hold the vectors.
    """
    in_class = SqlAlchemyParameter()
    id_field = SqlAlchemyParameter()
    text_field = SqlAlchemyParameter()
    out_class = SqlAlchemyParameter()
    batchable = luigi.Parameter(default=f3p('batchables/nlp/bert_vectorize'))

    def __init__(self, *args, **kwargs):
        # Build some meta-kwargs to pass to Sql2BatchTask
        if 'kwargs' not in kwargs:
            kwargs['kwargs'] = {}
        # Extract the module and table name for these ORMs
        for arg_name in ('in_class', 'out_class'):
            _class = assert_and_retrieve_kwarg(kwargs, arg_name)
            module, tablename = inspect_orm(_class)
            kwargs['kwargs'][f'{arg_name}_module'] = module
            kwargs['kwargs'][f'{arg_name}_tablename'] = tablename
        # The name of the id and text fields
        for arg_name in ('id_field', 'text_field'):
            kwargs['kwargs'][f'{arg_name}_name'] = assert_and_retrieve_kwarg(kwargs, arg_name).key
        super().__init__(*args, **kwargs)
