from nesta.core.orms.orm_utils import db_session
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.mag_orm import FieldOfStudy as FoS

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
