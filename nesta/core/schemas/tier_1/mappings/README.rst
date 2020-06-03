Elasticsearch mappings
======================

Our methodology for constructing Elasticsearch mappings is described here. It is intended to minimise duplication of efforts and enforce standardisation when referring to a common dataset whilst being flexible to individual project needs. It is implied in our framework that a single :code:`dataset` can be used across many projects, and each project is mapped to a single :code:`endpoint`. It is useful to start by looking at the structure of the :code:`nesta/core/schemas/tier_1/mappings/` directory:

.. code-block:: bash

    .
    ├── datasets
    │   ├── arxiv_mapping.json
    │   ├── companies_mapping.json
    │   ├── cordis_mapping.json
    │   ├── gtr_mapping.json
    │   ├── meetup_mapping.json
    │   ├── nih_mapping.json
    │   └── patstat_mapping.json
    ├── defaults
    │   ├── index.json
    │   └── settings.json
    └── endpoints
	├── arxlive
	│   └── arxiv_mapping.json
	├── eurito
	│   ├── arxiv_mapping.json
	│   ├── companies_mapping.json
	│   └── patstat_mapping.json
	└── health-scanner
	    ├── aliases.json
	    ├── config.yaml
	    ├── nih_mapping.json
	    └── nulls.json

Firstly, consider the contents of the :code:`defaults` subdirectory:

- :code:`index.json` should contain default :code:`settings.index` information for all mappings. At time of writing, this is actually just a blank file, since we seldom override default shard sizes, and such like.
- :code:`settings.json` should contain all other default :code:`settings` fields for all mappings - for example analyzers.

Next consider the :code:`datasets` subdirectory. Each mapping file in here should contain the complete :code:`mappings` field for the respective dataset. The naming convention :code:`<dataset>_mapping.json` is a hard requirement, as :code:`<dataset>` will map to the index for this :code:`dataset` at any given :code:`endpoint`.

Finally consider the :code:`endpoints` subdirectory. Each sub-subdirectory here should map to any :code:`endpoint` which requires changes beyond the :code:`defaults` and :code:`datasets` mappings. Each mapping file within each :code:`endpoint` sub-subdirectory (e.g. :code:`arxlive` or :code:`health-scanner`) should satisfy the same naming convention (:code:`<dataset>_mapping.json`). All conventions here are also consistent with the :code:`elasticsearch.yaml` configuration file (to see this configuration, you will need to clone the repo and follow `these steps <https://nesta.readthedocs.io/en/dev/nesta.core.troubleshooting.html#where-is-the-latest-config>`_ to unencrypt the config), which looks a little like this:


.. code-block:: yaml

    ## The following assumes the AWS host endpoing naming convention:
    ## {scheme}://search-{endpoint}-{id}.{region}.es.amazonaws.com
    defaults:
      scheme: https
      port: 443
      region: eu-west-2
      type: _doc
    endpoints:
      # -------------------------------
      # <AWS endpoint domain name>:
      #   id: <AWS endpoint UUID>
      #   <default override key>: <default override value>  ## e.g.: scheme, port, region, _type
      #   indexes:
      #     <index name>: <incremental version number>  ## Note: defaults to <index name>_dev in testing mode
      # -------------------------------
      arxlive:
	id: <this is a secret>
	indexes:
	  arxiv: 4
      # -------------------------------
      health-scanner:
	id: <this is a secret>
	indexes:
	  nih: 6
	  companies: 5
	  meetup: 4
    ... etc ...

Note that for the :code:`health-scanner` endpoint, :code:`companies` and :code:`meetup` will be generated from the :code:`datasets` mappings, as they are not specified under the :code:`endpoints/health-scanner` subdirectory. Also note that :code:`endpoints` sub-directories do not need to exist for each :code:`endpoint` to be generated: the mappings will simply be generated from the dataset defaults. For example, a new endpoint :code:`general` can be generated from the DAPS codebase using the above, even though there is no :code:`endpoints/general` sub-subdirectory.

Individual :code:`endpoints` can also specify :code:`aliases.json` which harmonises field names across datasets for specific endpoints. This uses a convention as follows:

.. code-block:: python

    {
	#...the convention is...
	"<new field name>": {
	    "<dataset 1>": "<old field name 1>",
	    "<dataset 2>": "<old field name 2>",
	    "<dataset 3>": "<old field name 3>"
	},
	#...an example is...
	"city": {
	    "companies": "placeName_city_organisation",
	    "meetup": "placeName_city_group",
	    "nih": "placeName_city_organisation"
	},
	#...etc...#
    }

By default, this applies (what Joel calls) a "soft" alias, which is an `Elasticsearch alias <https://www.elastic.co/guide/en/elasticsearch/reference/current/alias.html>`_, however by specifying :code:`hard-alias=true` in :code:`config.yaml` (see :code:`health-scanner` above), the alias is instead applied directly (i.e. field names are physically replaced, not aliased).

You will also notice the :code:`nulls.json` file in the :code:`health-scanner` endpoint. This is a relatively experimental feature for automatically nullifying values on ingestion through ElasticsearchPlus, in lieu of proper exploratory data analysis. The logic and format for this `is documented here <https://github.com/nestauk/nesta/blob/dev/nesta/core/luigihacks/elasticsearchplus.py#L414>`_.

Mapping construction hierarchy
------------------------------

Each mapping is constructed by overriding nested fields using the :code:`defaults` :code:`datasets` and :code:`endpoints`, in that order (i.e. :code:`endpoints` override nested fields in :code:`datasets`, and :code:`datasets` override those in :code:`defaults`). If you would like to "switch off" a field from the :code:`defaults` or :code:`datasets` mappings, you should set the value of the nested field to :code:`null`. For example:

.. code-block:: javascript

    {
	"mappings": {
	    "_doc": {
		"dynamic": "strict",
		"properties": {
		    "placeName_zipcode_organisation": null
		}
	    }
	}
    }

will simply "switch off" the field :code:`placeName_zipcode_organisation`, which was specified in :code:`datasets`.

The logic for the mapping construction hierarchy is demonstrated in the respective :code:`orms.orm_utils.get_es_mapping` function:


.. code-block:: python

    def get_es_mapping(dataset, endpoint):
	'''Load the ES mapping for this dataset and endpoint,
	including aliases.

	Args:
	    dataset (str): Name of the dataset for the ES mapping.
	    endpoint (str): Name of the AWS ES endpoint.
	Returns:
	    :obj:`dict`
	'''
	mapping = _get_es_mapping(dataset, endpoint)
        _apply_alias(mapping, dataset, endpoint)
	_prune_nested(mapping)  # prunes any nested keys with null values
	return mapping

Integrated tests
----------------

The following :code:`pytest` tests are made (and triggered on PR via travis):

- :code:`aliases.json` files are checked for consistency with available :code:`datasets`.
- All mappings for each in :code:`datasets` and :code:`endpoints` are fully generated, and tested for compatibility with :code:`schema_transformations` (which is, in turn, checked against the valid ontology in :code:`tier_1.json`).

Features in DAPS2
-----------------

- The index version (e.g. :code:`'arxiv': 4` in :code:`elasticsearch.yaml`) will be automatically generated from semantic versioning and the git hash in DAPS2, therefore the :code:`indexes` field will consolidate to an itemised list of indexes.
- The mappings under :code:`datasets` will be automatically generated from the open ontology which will be baked into the tier-0 schemas. This will render :code:`schema_transformations` redundant.
- Elasticsearch components will be factored out of :code:`orm_utils`.
