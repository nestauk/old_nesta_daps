#!/usr/bin/env bash

# clean up old containers
docker container prune -f > /dev/null
# launch containerised luigi with the gtr pipeline
docker-compose -f ~/nesta/docker/docker-compose-gtr-dev.yml run -d luigi --module nesta.core.routines.gtr.gtr_collect GtrOnlyRootTask --split-collection --production
