#!/bin/bash

pipreqs --use-local --ignore scripts,schemas,docs,.travis,.git --print . | sort -f > requirements.txt
