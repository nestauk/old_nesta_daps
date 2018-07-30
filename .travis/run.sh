#!/bin/bash

set -e
set -x

# Setup test data
TOPDIR=$PWD
mkdir $TOPDIR/packages/meetup/data
cd $TOPDIR/packages/meetup/data
wget http://thematicmapping.org/downloads/TM_WORLD_BORDERS_SIMPL-0.3.zip
unzip TM_WORLD_BORDERS_SIMPL-0.3.zip
cd $PWD

# Set paths, and check they work
export WORLD_BORDERS=$TOPDIR/packages/meetup/data/TM_WORLD_BORDERS_SIMPL-0.3.shp
ls $WORLD_BORDERS

# Run every test
for TOPDIRNAME in packages production tools;
do
    TESTDIRS=$(find $TOPDIRNAME -name "test*" -type d)
    for TESTDIRNAME in $TESTDIRS;
    do
	python -m unittest discover $TESTDIRNAME
    done
done
