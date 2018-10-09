#!/bin/bash

pipreqs --force --ignore docs/ .
pip freeze | grep "PyMySQL" >> requirements.txt

