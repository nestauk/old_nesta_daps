#!/bin/bash

pipreqs --ignore docs/ --print . | sort -f > requirements.txt
