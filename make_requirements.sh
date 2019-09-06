#!/bin/bash

pipreqs  --ignore scripts,schemas,docs,.travis,.git,docker --print . | sort -f > requirements.txt
