Crunchbase data (private companies)
===================================

**NB: The Crunchbase pipeline may not work until** `this issue <https://github.com/nestauk/nesta/issues/199>`_ **has been resolved.**

Batchables for the collection and processing of Crunchbase data. As documented under `packages` and `routines`, 
the pipeline is executed in the following order (documentation for the `run.py` files is given below, which isn't super-informative. You're better off looking under packages and routines).

The data is collected from proprietary data dumps, parsed into MySQL (tier 0) and then piped into Elasticsearch (tier 1), post-processing.

.. automodule:: core.batchables.crunchbase.crunchbase_collect.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.crunchbase.crunchbase_elasticsearch.run
    :members:
    :undoc-members:
    :show-inheritance:
