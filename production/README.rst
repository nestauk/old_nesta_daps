Production
==========

Nesta's production system is based on Luigi_ pipelines, and are designed to be entirely
run on AWS via the batch service. The main Luigi server runs on a persistent EC2 instance.
Beyond the well documented Luigi code, the main features of the nesta production system are:

.. _Luigi: https://luigi.readthedocs.io/en/stable/

- :code:`luigihacks.autobatch`, which facilates a managed :code:`Luigi.Task` which is split,
  batched and combined in a single step. Currently only synchronous jobs are
  accepted. Asynchonous jobs (where downstream :code:`Luigi.Task` jobs can be triggered)
  are a part of a longer term plan.
- :code:`scripts.nesta_prepare_batch` which zips up the batchable with the specified environmental files and ships it to AWS S3.
- :code:`scripts.nesta_docker_build` which builds a specified docker environment and ships it to AWS ECS.
