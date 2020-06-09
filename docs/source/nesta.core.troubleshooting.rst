FAQ
===

Where `is <https://www.theguardian.com/news/datablog/2010/jul/16/data-plural-singular>`_ the data?
--------------------------------------------------------------------------------------------------

As a general rule-of-thumb, our data is always stored in the London region (:obj:`eu-west-2`), in either RDS (tier-0, MySQL) or Elasticsearch (tier-1). For the EURITO project we also use Neo4j (tier-1), and in the distant future we will use Neo4j for tier-2 (i.e. a knowledge graph).

Why don't you use Aurora rather than MySQL?
-------------------------------------------

Aurora is definitely cheaper for stable production and business processes but not for research and development. You are charged for every byte of data you have *ever* consumed. This quickly spirals out-of-control for big-data development. Maybe one day we'll consider migrating back, once the situation stabilises.

Where are the production machines?
----------------------------------

Production machines (EC2) run in Ohio (us-east-2).


Where is the latest config?
---------------------------

We use :obj:`git-crypt` to encrypt our configuration files whilst allowing them to be versioned in git (meaning that we can also rollback configuration). To unlock the configuration encryption, you should install :obj:`git-crypt`, then run :obj:`bash install.sh` from the project root, and finally unlock the configuration using the key found `here <s3://nesta-production-config/config.key>`_.


Where do I start with Elasticsearch?
------------------------------------

All Elasticsearch indexes (aka "databases" to the rest of the world), mappings (aka "schemas")  and whitelisting can be found `here <https://eu-west-2.console.aws.amazon.com/es/home?region=eu-west-2#>`_.

I’d recommend using PostMan for spinning up and knocking down indexes. Practice this on a new cluster (which you can spin up from the above link), and then practice :obj:`PUT`, :obj:`POST` and :obj:`DELETE` requests to :obj:`PUT` an index (remember: "database") with a mapping ("schema"), inserting a "row" with :obj:`POST` and then deleting the index with :obj:`DELETE`. You will quickly learn that it's very easy to delete everything in Elasticsearch.

Troubleshooting
===============


I'm having problems using the config files!
-------------------------------------------

We use :obj:`git-crypt` to encrypt our configuration files whilst allowing them to be versioned in git (meaning that we can also rollback configuration). To unlock the configuration encryption, you should install :obj:`git-crypt`, then run :obj:`bash install.sh` from the project root, and finally unlock the configuration using the `key <s3://nesta-production-config/config.key>`_.



How do I restart the apache server after downtime?
--------------------------------------------------

:code:`sudo service httpd restart`


How do I restart the luigi server after downtime?
-------------------------------------------------

:code:`sudo su - luigi`

:code:`source activate py36`

:code:`luigid --background --pidfile /var/run/luigi/luigi.pid --logdir /var/log/luigi`

How do I perform initial setup to ensure the batchables will run?
-----------------------------------------------------------------

- AWS CLI needs to be installed and configured:

:code:`pip install awscli`

:code:`aws configure`

AWS Access Key ID and Secret Access Key are set up in IAM > Users > Security Credentials
Default region name should be :code:`eu-west-1` to enable the error emails to be sent
In AWS SES the sender and receiver email addresses need to be verified

- The config files need to be accessible and the PATH and LUIGI_CONFIG_PATH
  need to be amended accordingly

How can I send/receive emails from Luigi?
-----------------------------------------

You should set the environmental variable :code:`export LUIGI_EMAIL="<your.email@something>"` in your :code:`.bashrc`. You can test this with :code:`luigi TestNotificationsTask --local-scheduler --email-force-send`. Make sure your email address has been registered under AWS SES.

How do I add a new user to the server?
--------------------------------------

- add the user with :code:`useradd --create-home username`
- add sudo privileges `following these instructions <https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux_OpenStack_Platform/2/html/Getting_Started_Guide/ch02s03.html>`_
- add to ec2 user group with :code:`sudo usermod -a -G ec2-user username`
- set a temp password with :code:`passwd username`
- their home directory will be :code:`/home/username/`
- copy :code:`.bashrc` to their home directory
- create folder :code:`.ssh` in their home directory
- copy :code:`.ssh/authorized_keys` to the same folder in their home directory (DONT MOVE IT!!)
- :code:`cd` to their home directory and perform the below
- chown their copy of :code:`.ssh/authorized_keys` to their username: :code:`chown username .ssh/authorized_keys`
- clone the nesta repo
- copy :code:`core/config` files
- set password to be changed next login :code:`chage -d 0 username`
- share the temp password and core pem file

If necessary:
- :code:`sudo chmod g+w /var/tmp/batch`
