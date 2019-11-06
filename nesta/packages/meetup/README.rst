Meetup
======

**NB: The meetup pipeline will not work until** `this issue <https://github.com/nestauk/nesta/issues/117>`_ **has been resolved.**

Data collection of Meetup data. The procedure starts with a single country and `Meetup category <https://secure.meetup.com/meetup_api/console/?path=/2/categories>`_. All of the groups within the country are discovered, from which all members are subsequently retrieved (no personal information!). In order to build a fuller picture, all other groups to which the members belong are retrieved, which may be in other categories or countries. Finally, all group details are retrieved.

The code should be executed in the following order, which reflects the latter procedure:

1) country_groups.py
2) groups_members.py
3) members_groups.py
4) groups_details.py

Each script generates a list of dictionaries which can be ingested by the proceeding script.
