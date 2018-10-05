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
