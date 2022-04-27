#!/bin/bash

repo_tag=apache/spark:v3.2.1

docker run --rm               \
  --user root                 \
  -v $PWD:/opt/spark/work-dir \
  -w /opt/spark/work-dir      \
  $repo_tag bash check.sh
