Elasticsearch
=============

The following steps will take you through setting up elasticsearch on an EC2
instance.

Launch the EC2 instance and ssh in so the following can be installed:

docker
------
:code:`sudo yum install docker -y`

docker-compose
--------------
``curl -L https://github.com/docker/compose/releases/download/1.22.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose``
:code:`chmod +x /usr/local/bin/docker-compose`
    more info: https://github.com/docker/compose/releases

docker permissions
------------------
:code:`sudo usermod -a -G docker $USER`
    more info: https://techoverflow.net/2017/03/01/solving-docker-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket/

vm.max_map_count
----------------
set permanantly in */etc/sysctl.conf* by adding the following line:
:code:`vm.max_map_count=262144`
    more info: https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html

python 3.6
----------
:code:`sudo yum install python36 -y`

*The machine now needs to be rebooted*
:code:`sudo reboot now`

Docker
------
- max file descriptors
- docker-compose

