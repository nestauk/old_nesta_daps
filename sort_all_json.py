from pathlib import Path
import json

if __name__ == "__main__":    
    for fname in Path('nesta').glob('**/*.json'):
        if 'task_chain' in str(fname):
            continue
        with open(fname) as f:
            try:
                js = json.load(f)
            except json.decoder.JSONDecodeError:
                print('Error on file', fname)
                raise
        with open(fname, "w") as f:
            json.dump(js, f, sort_keys=True, indent=4)
