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
:code:`curl -L https://github.com/docker/compose/releases/download/1.22.0/docker-compose-\`uname -s\` - \`uname -m\` -o /usr/local/bin/docker-compose`
:code:`chmod +x /usr/local/bin/docker-compose`
:code:`sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose`

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
the :code:`docker-compose.yml` needs to include ulimits settings::
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
          soft: 65536
          hard: 65536

Recipes for http or https clusters can be found in: :code:`nesta/production/scripts/elasticsearch`

There is also an EC2 AMI for a http node stored in the London region: :code:`elasticsearch node London vX`

Reindexing data from a remote cluster
-------------------------------------
- reindex permissions need to be set in the new cluster's *elasticsearch.yml*
- if the existing cluster is AWS hosted ES the ip address needs to be added to
  the security settings
- follow this guide: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#reindex-from-remote
- *index* and *query* do not need to be supplied
- if reindexing from AWS ES the port should be 443 for https. This is mandatory in the json sent to the reindexing api

