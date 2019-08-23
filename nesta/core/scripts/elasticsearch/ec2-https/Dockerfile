FROM docker.elastic.co/elasticsearch/elasticsearch:6.4.2

RUN bin/elasticsearch-plugin install discovery-ec2 --batch
RUN bin/elasticsearch-plugin install repository-s3 --batch

RUN mkdir config/certs
ADD ./elasticsearch.yml config/elasticsearch.yml
ADD ./elastic-certificates.p12 config/certs/elastic-certificates.p12

USER root
RUN chown elasticsearch:elasticsearch config/certs/elastic-certificates.p12
USER elasticsearch
