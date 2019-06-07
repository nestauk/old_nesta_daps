#!/usr/bin/env bash

# pass any arguments straight on to luigi
luigi --local-scheduler "$@"
