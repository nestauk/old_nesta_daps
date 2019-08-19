import os
import json
from collections import Counter
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub

ES_CONF_SUFFIX = "_es_config.json"

def alias_info(filepath):
    with open(filepath) as f:
        data = json.load(f)        
    for alias, info in data.items():
        for dataset, field in info.items():
            yield (alias, dataset, field)


class TestValidate():
    def test_validate(self):
        # Load the ontology
        cwd = os.path.dirname(__file__)
        filename = os.path.join(cwd, '../tier_1.json')
        with open(filename) as f:
            data = json.load(f)        
        ontology = {row["term"]: row["values"] for row in data}
        # Assert the core structure of the ontology
        assert len(ontology) == 3
        for term_type in ["firstName", "middleName", "lastName"]:
            assert term_type in ontology

        # Iterate over schema transformations
        dirname = os.path.join(cwd, '../schema_transformations/')
        all_fields = {}
        for filename in os.listdir(dirname):            
            # Load the transformation
            if not filename.endswith(".json"):
                continue
            print(filename)
            filename = os.path.join(dirname, filename)
            with open(filename) as f:
                data = json.load(f)
            # Assert that the terms are in the ontology
            tier_0, tier_1 = [], []
            for row in data:
                fieldname = row['tier_1']
                tier_0.append(row['tier_0'])
                tier_1.append(fieldname)
                if fieldname.startswith("_"):
                    fieldname = fieldname[1:]
                first, middle, last = fieldname.split("_")
                assert first in ontology["firstName"]
                assert middle in ontology["middleName"]
                assert last in ontology["lastName"]
            # Record the dataset name for the next tests
            dataset_name = filename.replace(".json", "").split("/")[-1]
            all_fields[dataset_name] = tier_1
            # Assert no duplicates
            _, count = Counter(tier_0).most_common(1)[0]
            print(Counter(tier_0).most_common(1)[0])
            assert count == 1
            _, count = Counter(tier_1).most_common(1)[0]
            print(Counter(tier_1).most_common(1)[0])
            assert count == 1

    def test_aliases(self):    
        """Assert consistency between the aliases and schemas"""
        top_dir = find_filepath_from_pathstub("production/orms")
        all_fields = {}
        for filename in os.listdir(top_dir):
            if not filename.endswith(ES_CONF_SUFFIX):
                continue
            dataset = filename.replace(ES_CONF_SUFFIX, "")
            filename = os.path.join(top_dir, filename)            
            with open(filename) as f:
                data = json.load(f)
            fields = data["mappings"]["_doc"]["properties"].keys()
            all_fields[dataset] = fields

        cwd = os.path.dirname(__file__)
        path = os.path.join(cwd, '../aliases/')
        for filename in os.listdir(path):
            if not filename.endswith(".json"):
                continue
            print(filename)
            filename =  os.path.join(path, filename)
            for alias, dataset, field in alias_info(filename):
                print("\t", alias, dataset, field)
                assert dataset in all_fields.keys()
                assert field in all_fields[dataset]
