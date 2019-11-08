#!/bin/bash

set -e
set -x

# Setup test data
TOPDIR=$PWD
mkdir $TOPDIR/nesta/packages/meetup/data
cd $TOPDIR/nesta/packages/meetup/data
wget http://thematicmapping.org/downloads/TM_WORLD_BORDERS_SIMPL-0.3.zip
unzip TM_WORLD_BORDERS_SIMPL-0.3.zip
cd $TOPDIR

# Set paths, and check they work
export WORLD_BORDERS=$TOPDIR/nesta/packages/meetup/data/TM_WORLD_BORDERS_SIMPL-0.3.shp
ls $WORLD_BORDERS

# Run every test
for TOPDIRNAME in core packages;
do
    TESTDIRS=$(find nesta/$TOPDIRNAME -name "test*" -type d)
    for TESTDIRNAME in $TESTDIRS;
    do
	pytest --disable-pytest-warnings $TESTDIRNAME
    done
done
