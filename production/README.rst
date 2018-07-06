Production
==========

Nesta's production system, based on Luigi on AWS. The main features are:

- `luigihacks.autobatch`, which facilates a managed :code:`Luigi.Task` which is split,
  batched and combined in a single step. Currently only synchronous jobs are
  accepted. Asynchonous jobs (where downstream :code:`Luigi.Task` jobs can be triggered)
  are a part of a longer term plan.
- `scripts.nesta_prepare_batch` (if you're on readthedocs, see the GitHub), which zips up
  the batchable with the specified environmental files.
- `scripts.nesta_docker_build` which 

More details to follow.

Productionising
---------------

1. Audit the package code, required to pass all auditing tests
2. Understand what environment is required
3. Write a Dockerfile and docker launch script for this under scripts/docker_recipes
4. Build the Docker environment (run:      docker_build <recipe_name>  from any directory)
5. Build and test the batchable(s)
6. Build and test a Luigi pipeline
7. [...] Need to have steps here which estimate run time cost parameters. Could use tests.py to estimate this. [...]
8. Run the full chain


