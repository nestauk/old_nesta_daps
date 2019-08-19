.. include:: ../../nesta/core.README.rst

How to put code into production at nesta
----------------------------------------

Note: this is being actively developed.

1. Audit the package code, required to pass all auditing tests
2. Understand what environment is required
3. Write a Dockerfile and docker launch script for this under scripts/docker_recipes
4. Build the Docker environment (run:      docker_build <recipe_name>  from any directory)
5. Build and test the batchable(s)
6. Build and test a Luigi pipeline
7. [...] Need to have steps here which estimate run time cost parameters. Could use tests.py to estimate this. [...]
8. Run the full chain

	     
.. automodule:: core
    :members:
    :undoc-members:
    :show-inheritance:

Code and scripts
----------------

.. toctree::

    nesta.core.routines
    nesta.core.batchables
    nesta.core.orms   
    nesta.core.schemas 
    nesta.core.luigihacks
    nesta.core.scripts
    nesta.core.elasticsearch
    nesta.core.containers
    nesta.core.troubleshooting
