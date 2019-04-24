import os
import json
from collections import Counter

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
                tier_0.append(row['tier_0'])
                tier_1.append(row['tier_1'])
                first, middle, last = row['tier_1'].split("_")
                assert first in ontology["firstName"]
                assert middle in ontology["middleName"]
                assert last in ontology["lastName"]
            # Assert no duplicates
            _, count = Counter(tier_0).most_common(1)[0]
            assert count == 1
            _, count = Counter(tier_1).most_common(1)[0]
            assert count == 1
