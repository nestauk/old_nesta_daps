Routines
========

All of our pipelines, implemented as Luigi routines. Some of these pipelines (at least partly) rely on batch computing (via AWS Batch), where the 'batched' scripts (`run.py` modules) are described in :code:`core.batchables`. Other than :code:`luigihacks.autobatch`, which is respectively documented, the routine procedure follows the core Luigi_ documentation.

.. _Luigi: https://luigi.readthedocs.io/en/stable/
