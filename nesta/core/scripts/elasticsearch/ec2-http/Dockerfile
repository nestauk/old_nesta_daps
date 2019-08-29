FROM docker.elastic.co/elasticsearch/elasticsearch:6.4.2

RUN bin/elasticsearch-plugin install discovery-ec2 --batch
RUN bin/elasticsearch-plugin install repository-s3 --batch

ADD ./elasticsearch.yml config/elasticsearch.yml
