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
- copy :code:`production/config` files
- set password to be changed next login :code:`chage -d 0 username`
- share the temp password and core pem file

If necessary:
- :code:`sudo chmod g+w /var/tmp/batch`
