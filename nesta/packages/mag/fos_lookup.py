from nesta.core.orms.orm_utils import db_session
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.mag_orm import FieldOfStudy as FoS
#import networkx as nx

def split_ids(child_ids):
    if child_ids is None:
        return []
    return [int(x) for x in child_ids.split(',')]


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


# def get_outer_nodes(g, top_most=True): 
#     degrees = g.in_degree if top_most else g.out_degree
#     return (node for node, degree in degrees 
#             if degree == 0) 


# def make_fos_paths(fos_rows, fos_lookup): 
#     """
#     fos_rows = row.pop('fields_of_study') 
#     fos_paths = make_fos_paths(fos_rows, 
#     fos_lookup)
#     """
#     ids = set(fos['id'] for fos in fos_rows) 
#     edges = [fos_lookup[(row['id'], cid)]    
#              for row in fos_rows 
#              for cid in split_ids(row['child_ids']) 
#              if cid in ids] 
#     g = nx.DiGraph(edges) 
#     paths = [] 
#     for parent in get_outer_nodes(g): 
#         for child in get_outer_nodes(g, False): 
#             paths += nx.all_simple_paths(g, parent, child)
#     return sorted(paths) 
