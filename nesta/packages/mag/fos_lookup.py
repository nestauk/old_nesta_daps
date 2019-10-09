from nesta.core.orms.orm_utils import db_session
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.mag_orm import FieldOfStudy as FoS

from more_itertools import unique_everseen
from collections import defaultdict
import json

class UniqueList(list):
    def append(self, item):
        if item not in self:
            super().append(item)


def unique_index(item, pairs): #, first=True):
    first_items = (it[0] for it in pairs)
    uniques = list(unique_everseen(first_items))
    return uniques.index(item)


def find_index(field, fos_map):
    for i_level, fields in fos_map.items():
        try:
            return i_level, unique_index(field, fields)
        except ValueError:
            pass


def make_fos_map(fos_rows, fos_lookup):
    ids = set(fos['id'] for fos in fos_rows)
    fos_map = defaultdict(UniqueList)
    for row in fos_rows:
        # Get child ids and remove missing ids
        parent_ids = split_ids(row['parent_ids'], ids)
        child_ids = split_ids(row['child_ids'], ids)        
        # Iterate over children
        for cid in child_ids:
            parent, child = fos_lookup[(row['id'], cid)]
            level = row['level']
            if len(parent_ids) == 0: 
                fos_map[level].append([parent])
            fos_map[level + 1].append([child, parent])
    return fos_map


def make_fos_nodes(fos_map):
    nodes = defaultdict(dict)
    links = []
    for _, fields in fos_map.items():
        for row in fields:
            child = row[0]
            parent = row[-1]  # Note: child could equal parent
            i_level, i_idx = find_index(child, fos_map)
            if child != parent:
                j_level, j_idx = find_index(parent, fos_map)
                links.append([[j_level, j_idx],
                              [i_level, i_idx]])
            nodes[i_level][i_idx] = child
    nodes = json.loads(json.dumps(nodes, sort_keys=True))
    return {'nodes': nodes, 'links': sorted(links)}


def split_ids(child_ids, existing=None):
    if child_ids is None:
        return set()
    found = set(int(x) for x in child_ids.split(','))
    if existing is None:
        existing = found
    missing = found - existing
    return found - missing


def build_fos_lookup(engine, max_lvl=2):
    with db_session(engine) as session:
        fos = [f.__dict__ for f in (session.query(FoS)
                                    .filter(FoS.level <= max_lvl)
                                    .all())]
    fos_children = {f['id']: split_ids(f['child_ids'])
                    for f in fos}
    fos_names = {f['id']: f['name'] for f in fos}    
    return {(pid, cid): [fos_names[pid], fos_names[cid]]
            for pid, children in fos_children.items()
            for cid in children if cid in fos_children}
