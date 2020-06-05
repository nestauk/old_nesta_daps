from nesta.core.orms.orm_utils import get_es_mapping
import os
import glob
import json
import pytest

@pytest.fixture
def json_files():
    cwd = os.path.dirname(__file__)
    return glob.glob(f'{cwd}/../**/*json', recursive=True)


def test_is_tidy(json_files):
    """Check that all files are valid, tidy json"""
    for filename in json_files:
        # ontology.json is tested elsewhere
        _, _filename = os.path.split(filename)
        if _filename == 'ontology.json':
            continue
        with open(filename) as f:
            raw = f.read() 
            js = json.loads(raw)
        assert raw == json.dumps(js, sort_keys=True, indent=4), (f'\n\n{_filename} has not been tidied.\nBe sure to '
                                                                 'run "python .githooks/hooktools/sort_all_json.py" '
                                                                 'from the root directory to '
                                                                 'avoid this test failure.\n\n')

def test_mappings_build(json_files):
    endpoints, datasets = set(), set()
    for filename in json_files:
        if not filename.endswith('mapping.json'):
            continue
        if 'datasets' in filename:
            _, _filename = os.path.split(filename)
            dataset = _filename.split('_mapping.json')[0]
            datasets.add(dataset)
        if 'endpoints' in filename:
            dirname, _ = os.path.split(filename)
            _, endpoint = os.path.split(dirname)
            endpoints.add(endpoint)
    for endpoint in endpoints:
        for dataset in datasets:
            get_es_mapping(dataset, endpoint)
    get_es_mapping(dataset[0], 'dummy') # <--- also test on non-existent endpoint
