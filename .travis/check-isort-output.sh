#!/bin/bash
output="$(isort --recursive --check-only --diff python/ray/)"
success="^Skipped [0-9]+ files$"
if [[ "$output" =~ $success ]] ; then
  echo "isort check passed."
  exit 0
else
  echo "isort check failed:"
  echo "$output"
  exit 1
fi
