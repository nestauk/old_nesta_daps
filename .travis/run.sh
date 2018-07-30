#!/bin/bash

set -e
set -x

# Setup test data
mkdir $HOME/nesta/packages/meetup/data
cd $HOME/nesta/packages/meetup/data

wget http://thematicmapping.org/downloads/TM_WORLD_BORDERS_SIMPL-0.3.zip

cd -


# Set paths, and check they work
export WORLD_BORDERS=$HOME/nesta/packages/meetup/data/TM_WORLD_BORDERS_SIMPL-0.3.shp
export LUIGI_CONFIG_PATH=$HOME/nesta/production/config/luigi.cfg
ls $WORLD_BORDERS
ls $LUIGI_CONFIG_PATH

# Run every test
for TOPDIRNAME in packages production tools;
do
    TESTDIRS=$(find $TOPDIRNAME -name "test*" -type d)
    for TESTDIRNAME in $TESTDIRS;
    do
	python -m unittest discover $TESTDIRNAME
    done
done
