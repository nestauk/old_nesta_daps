#!/usr/bin/env bash

chmod +x githooks/pre-commit
bash githooks/pre-commit && ln -s githooks/pre-commit .git/hooks/pre-commit
echo "You should now run 'git-crypt unlock /path/to/key' to access config files"
