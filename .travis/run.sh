#!/bin/bash

set -e
set -x

# Setup test data
TOPDIR=$PWD
mkdir $TOPDIR/packages/meetup/data
cd $TOPDIR/packages/meetup/data
wget http://thematicmapping.org/downloads/TM_WORLD_BORDERS_SIMPL-0.3.zip
cd $PWD

# Set paths, and check they work
export WORLD_BORDERS=$TOPDIR/packages/meetup/data/TM_WORLD_BORDERS_SIMPL-0.3.shp
export LUIGI_CONFIG_PATH=$TOPDIR/production/config/luigi.cfg
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
