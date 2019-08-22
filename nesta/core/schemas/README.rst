Ontologies and schemas
======================

Tier 0
------

Raw data collections ("tier 0") in the production system do not adhere to a fixed schema or ontology, but instead have a schema which is very close to the raw data. Modifications to field names tend to be quite basic, such as lowercase and removal of whitespace in favour of a single underscore.

Tier 1
------

Processed data ("tier 1") is intended for public consumption, using a common ontology. The convention we use is as follows:

- Field names are composed of up to three terms: a :code:`firstName`, :code:`middleName` and :code:`lastName`
- Each term (e.g. :code:`firstName`) is written in lowerCamelCase.
- :code:`firstName` terms correspond to a restricted set of basic quantities.
- :code:`middleName` terms correspond to a restricted set of modifiers (e.g. adjectives) which add nuance to the :code:`firstName` term. Note, the special :code:`middleName` term :code:`of` is reserved as the default value in case no :code:`middleName` is specified.
- :code:`lastName` terms correspond to a restricted set of entity types.

Valid examples are :code:`date_start_project` and :code:`title_of_project`.

Tier 0 fields are implictly excluded from tier 1 if they are missing from the :code:`schema_transformation` file. Tier 1 schema field names are applied via `nesta.packages.decorator.schema_transform`

Tier 2
------

Although not-yet-implemented, the tier 2 schema is reserved for future graph ontologies. Don't expect any changes any time soon!
