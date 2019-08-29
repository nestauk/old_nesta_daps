import json
import luigi
from nesta.core.luigihacks.parameter import _DictParamEncoderPlus
from nesta.core.luigihacks.parameter import DictParameterPlus

def test_encoder():
    encoder = _DictParamEncoderPlus()
    task = luigi.Task()
    for value in [1, 1.0, 'a', (1,2), task]:
        encoder.default(value)

def test_serialize():
    task = luigi.Task()
    param = DictParameterPlus(default={'first': 1, 'second': 'a', 
                                       'third': task})
    _json = param.serialize(task)    
    assert type(_json) is str
    assert len(_json) > 0
