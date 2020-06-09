import os
from pathlib import Path
import pytest
import json

@pytest.fixture
def ontology():
    dirname = Path(os.path.dirname(__file__)).parent
    # Load the ontology                                                                                                       
    filename = os.path.join(dirname, 'ontology.json')
    with open(filename) as f:
        ontology = json.load(f)    
    return {row['term']: row['values'] for row in ontology}


def test_ontology_uniqueness(ontology):
    for lvl, values in ontology.items():
        assert len(values) == len(set(values)), f'{lvl} has duplicate values'


def test_validate(ontology):
    dirname = Path(os.path.dirname(__file__)).parent
    dataset_dirname = os.path.join(dirname, 'datasets')
    firsts, middles, lasts = [], [], []
    # Test each dataset for valid ontology
    for filename in os.listdir(dataset_dirname):
        print(filename)
        filename = os.path.join(dataset_dirname, filename)
        with open(filename) as f:
            dataset = json.load(f)
        for field_name in dataset['tier0_to_tier1'].values():
            if field_name.startswith('_'):
                field_name = field_name[1:]
            first, middle, last = field_name.split('_')
            # Test the vocab is valid
            assert first in ontology['firstName'], f'{dataset} has unexpected field {field_name}'
            assert middle in ontology['middleName'], f'{dataset} has unexpected field {field_name}'
            assert last in ontology['lastName'], f'{dataset} has unexpected field {field_name}'
            # Save these for the tests at the end
            firsts.append(first)
            middles.append(middle)
            lasts.append(last)
    # Test there is no superfluous vocab in the ontology
    for f in ontology['firstName']:
        assert f in firsts, f'Unused first name: {f}'
    for f in ontology['middleName']:
        assert f in middles, f'Unused middle name: {f}'
    for f in ontology['lastName']:
        assert f in lasts, f'Unused last name: {f}'            
