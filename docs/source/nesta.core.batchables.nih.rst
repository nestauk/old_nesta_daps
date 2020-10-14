NiH data (health research)
==========================

Batchables for the collection and processing of NiH data. As documented under `packages` and `routines`, 
the pipeline is executed in the following order (documentation for the `run.py` files is given below, which isn't super-informative. You're better off looking under packages and routines).

The data is collected from official data dumps, parsed into MySQL (tier 0) and then piped into Elasticsearch (tier 1), post-processing.

.. automodule:: core.batchables.health_data.nih_collect_data.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.health_data.nih_process_data.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.health_data.nih_abstract_mesh_data.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.health_data.nih_dedupe.run
    :members:
    :undoc-members:
    :show-inheritance:
