import luigi
from luigi.parameter import _DictParamEncoder
import json


class _DictParamEncoderPlus(_DictParamEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            pass
        if isinstance(obj, luigi.Task):
            return cls.get_task_family()

class DictParameterPlus(luigi.DictParameter):
    def __init__(self, encoder=_DictParamEncoderPlus, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoder = encoder

    def serialize(self, x):
        return json.dumps(x, cls=self.encoder)

