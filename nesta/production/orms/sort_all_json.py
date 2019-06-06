import os
import json

if __name__ == "__main__":
    for fname in os.listdir("."):
        if not fname.endswith(".json"):
            continue
        with open(fname) as f:
            js = json.load(f)
        with open(fname, "w") as f:
            json.dump(js, f, sort_keys=True, indent=4)
