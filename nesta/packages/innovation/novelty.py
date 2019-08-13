import numpy as np
from rhodonite.utils.graph import subgraph_eprop_agg

def novelty_score(g, vertices, k_eprop):
    return subgraph_eprop_agg(g, vertices, k_eprop, aggfuncs=[np.mean])[0]
