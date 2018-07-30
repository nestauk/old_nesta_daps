#!/bin/bash

set -e
set -x

echo $PWD
for TOPDIRNAME in packages production tools;
do
    TESTDIRS=$(find $TOPDIRNAME -name "test*" -type d)
    for TESTDIRNAME in $TESTDIRS;
    do
	python -m unittest discover $TESTDIRNAME
    done
done
