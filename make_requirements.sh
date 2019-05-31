#!/bin/bash

pipreqs   --ignore scripts,schemas,docs,.travis,.git --print . | sort -f > requirements.txt
