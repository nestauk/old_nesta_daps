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

