GtR (UK publicly funded research)
=================================

Batchable tools for collecting and processing GtR data. As documented under packages and routines, the pipeline is executed in the following order (documentation for the run.py files is given below, which isn’t super-informative. You’re better off looking under packages and routines).

The data is collected by traversing the graph exposed by the GtR API, and is parsed into MySQL (tier 0). There is a further module for directly generating document embeddings of GtR project descriptions, which can be used for finding topics.

.. automodule:: nesta.core.batchables.gtr.collect_gtr.run
    :members:
    :undoc-members:
    :show-inheritance:


.. automodule:: nesta.core.batchables.gtr.embed_topics.run
    :members:
    :undoc-members:
    :show-inheritance:
