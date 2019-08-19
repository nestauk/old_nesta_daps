Batchables
==========

Packets of code to be batched by :code:`production.routines` routines. Each packet should sit in it's own directory, with a file called :code:`run.py`, containing a 'main' function called :code:`run()` which will be executed on the AWS batch system.

Each `run.py` should expect an environment parameter called :code:`BATCHPAR_outfile` which should provide information on the output location. Other input parameters should be prefixed with :code:`BATCHPAR_`, as set in :code:`production.routines` routine.
