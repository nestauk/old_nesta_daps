"""
parameter
=========

Heavily based on :py:class:`luigi.parameter`. This package
extends the :py:class:`luigi.DictParameter` to allow dict values
t include :py:class:`luigi.Task`.
"""

import luigi
from luigi.parameter import _DictParamEncoder
import json


class _DictParamEncoderPlus(_DictParamEncoder):
    """
    JSON encoder for :py:class:`~DictParameterPlus`, which makes :py:class:`Task` JSON serializable.
    """
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            pass
        if isinstance(obj, luigi.Task):
            return obj.get_task_family()

class DictParameterPlus(luigi.DictParameter):
    """
    Parameter whose value is a ``dict` and whose values may include
    a :py:class:`Task`.
    """
    def __init__(self, encoder=_DictParamEncoderPlus, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoder = encoder

    def serialize(self, x):
        return json.dumps(x, cls=self.encoder)
