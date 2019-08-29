"""
tidy_schema
===========

Sorts and pretty prints the tier_1 schema.
Useful to do this to avoid Git conflicts.
"""

import json
FILENAME="tier_1.json"

# Load
with open(FILENAME) as f:
    js = json.load(f)

# Sort
for row in js:
    row['values'].sort()

# Save
with open(FILENAME,"w") as f:
    json.dump(js, f, indent=4)
