#!/usr/bin/env bash

# clean up old containers
docker container prune -f
# launch containerised luigi with the arxlive pipeline
docker-compose -f ~/nesta/docker/docker-compose-arxlive-dev.yml run -d luigi --module nesta.production.routines.arxiv.arxiv_root_task RootTask --production
