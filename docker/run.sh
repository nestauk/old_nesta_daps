#!/usr/bin/env bash
# uncomment to enable the central scheduler (maybe useful for the graphical interface)
# luigid &

# pass any arguments straight on to luigi
luigi --local-scheduler "$@"
