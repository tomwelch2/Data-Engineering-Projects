#!/bin/bash

PATH='/home/tom/Documents/python_files/git_project/pandas_files/pandas_pipelines'
if [[ -e "$PATH" ]]; then
  python3 "$PATH"
else
  echo "Error - file not found"
fi
