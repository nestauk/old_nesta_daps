.. include:: ../../nesta/packages/README.rst

.. automodule:: packages
    :members:
    :undoc-members:
    :show-inheritance:

Code and scripts
----------------

.. toctree::

    nesta.packages.meetup
    nesta.packages.nlp_utils


Code auditing
-------------

Packages are only accepted if they satisfy our internal auditing procedure:

- Common sense requirements:
   - Either:
       - The code produces at least one data or model output; **or**
       - The code provides a service which abstracts away significant complexity.
   - There is one unit test for each function or method, which lasts no longer than about 1 minute.
   - Each data or model output is produced from a single function or method, as described in the :code:`__main__` of a specified file.
   - Can the nearest programmer (or equivalent) checkout and execute your tests from scratch?
   - Will the code be used to perform non-project specific tasks?
   - Does the process perform a logical task or fulfil a logical purpose?


- If the code requires productionising, it satisfies one of the following conditions:
   a) There is a non-trivial pipeline, which would benefit from formal productionising.
   b) A procedure is foreseen to be reperformed for new contexts with atomic differences in run conditions.
   c) The output is a service which requires a pipeline.
   d) The process is a regular / longitudinal data collection.


- Basic PEP8 and style requirements:
   - Docstrings for every exposable class, method and function.
   - Usage in a README.rst or in Docstring at the top of the file.
   - CamelCase class names.
   - Underscore separation of all other variable, function and module names.
   - No glaring programming no-nos.
   - Never use :code:`print`: opt for :code:`logging` instead.


- Bureaucratic requirements:
   - A requirements file*.
   - The README file specifies the operating system and python version.
