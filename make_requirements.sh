#!/bin/bash

pipreqs --force --ignore docs/ .
pip freeze | grep "PyMySQL" >> requirements.txt
echo "setuptools>=40.4.3" >> requirements.txt
