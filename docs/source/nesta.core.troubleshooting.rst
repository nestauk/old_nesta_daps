AWS FAQ
=======

Where is the data?
------------------

As a general rule-of-thumb, our data is always stored in the London region (:obj:`eu-west-2`), in either RDS (MySQL)

Why don't you use Aurora rather than MySQL?
-------------------------------------------

Aurora is definitely cheaper for stable production and business processes but not for research and development. You are charged for every byte of data you have *ever* consumed. This quickly spirals out-of-control for big-data development. Maybe one day we'll consider migrating back, once the situation stabilises.

Where are the production machines?
----------------------------------

Production machines (EC2) run in Ohio (us-east-2).


Where is the latest config?
---------------------------

One of our priorities is to implement a `decent config management system <https://github.com/nestauk/nesta/issues/196>`_. Our latest read-only config for accessing the tier-0 (rawish SQL data) can be found `here <s3://nesta-production-config/mysqldb_team.config>`_, and a fairly up-to-date config directory (which you can paste into :obj:`nesta/core/config`) can be found `here <https://s3.console.aws.amazon.com/s3/object/nesta-production-config/nesta-config.zip?region=eu-west-2&tab=overview>`_. If you want to use exactly what Joel has been using, feel free to :obj:`sudo cp` from :obj:`/home/ec2-user/nesta/nesta/core/config`. For example, you can find the latest Elasticsearch indexes and endpoints here.

Where do I start with Elasticsearch?
------------------------------------

All Elasticsearch indexes (aka "databases" to the rest of the world), mappings (aka "schemas")  and whitelisting can be found `here <https://eu-west-2.console.aws.amazon.com/es/home?region=eu-west-2#>`_.

I’d recommend using PostMan for spinning up and knocking down indexes. Practice this on a new cluster (which you can spin up from the above link), and then practice :obj:`PUT`, :obj:`POST` and :obj:`DELETE` requests to :obj:`PUT` an index (remember: "database") with a mapping ("schema"), inserting a "row" with :obj:`POST` and then deleting the index with :obj:`DELETE`. You will quickly learn that it's very easy to delete everything in Elasticsearch.

Troubleshooting
===============

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
