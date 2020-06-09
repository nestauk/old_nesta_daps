import os
import glob
import json
from pathlib import Path
import pytest

@pytest.fixture
def json_files():
    cwd = os.path.dirname(__file__)
    return list(glob.glob(f'{cwd}/../**/*json', recursive=True))


def test_mappings_build(json_files):

    # Test each dataset for valid ontology
    dirname = Path(os.path.dirname(__file__)).parent
    dataset_dirname = os.path.join(dirname, 'datasets')
    ontology = {}
    for dataset in os.listdir(dataset_dirname):
        filename = os.path.join(dataset_dirname, dataset)
        with open(filename) as f:
            _ontology = json.load(f)
        ontology[dataset.split('.json')[0]] = list(_ontology['tier0_to_tier1'].values())
            
    # Test that each alias is valid
    for filename in json_files:
        _, _filename = os.path.split(filename)
        if _filename != 'aliases.json':
            continue
        with open(filename) as f:
            aliases = json.load(f)
        for new_name, info in aliases.items():
            for dataset, old_name in info.items():
                assert dataset in ontology, f'No such dataset "{dataset}" in {list(ontology.keys())}, referenced by {filename}'
                assert old_name in ontology[dataset], f'{old_name} not found in {dataset}, referenced by {filename}'
