Meetup
======

**NB: The meetup pipeline will not work until** `this issue <https://github.com/nestauk/nesta/issues/117>`_ **has been resolved.**

Batchables for the Meetup data collection pipeline. As documented under `packages` and `routines`, the pipeline is executed in the following order (documentation for the `run.py` files is given below, which isn't super-informative. You're better off looking under packages and routines).

The :obj:`topic_tag_elasticsearch` module is responsible for piping data to elasticsearch, as well as apply topic tags and filtering small groups out of the data.


.. automodule:: core.batchables.meetup.country_groups.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.meetup.groups_members.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.meetup.members_groups.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.meetup.group_details.run
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: core.batchables.meetup.topic_tag_elasticsearch.run
    :members:
    :undoc-members:
    :show-inheritance:
