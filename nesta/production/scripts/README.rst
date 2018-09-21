Scripts
====================

A set of helper scripts for the batching system.

Note that this directory is required to sit in `$PATH`. By convention, all executables in this directory
start with `nesta_` so that our developers know where to find them. 


nesta_prepare_batch
-------------------

Collect a batchable :code:`run.py` file, including dependencies and an automaticlly generated requirements file; which is all zipped up and sent to AWS S3 for batching. This script is executed automatically in :code:`luigihacks.autobatch.AutoBatchTask.run`.

**Parameters**:

- **BATCHABLE_DIRECTORY**: The path to the directory containing the batchable :code:`run.py` file.
- **ARGS**: Space-separated-list of files or directories to include in the zip file, for example imports.


nesta_docker_build
------------------

Build a docker environment and register it with the AWS ECS container repository.

**Parameters**:

- **DOCKER_RECIPE**: A docker recipe. See :code:`docker_recipes/` for a good idea of how to build a new environment.

