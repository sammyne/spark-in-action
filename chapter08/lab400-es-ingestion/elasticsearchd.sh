#!/bin/bash

cd $(dirname $0)

name=es-spark-in-action
#repo_tag=elasticsearch:8.2.0
#repo_tag=elasticsearch:6.8.23
repo_tag=elasticsearch:5.4.3

docker rm -f $name

docker run -t --rm                \
  --name $name                    \
  -e "discovery.type=single-node" \
  -e "path.repo=/testdata/nyc_restaurants" \
  -v $PWD/testdata/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml \
  -p 9200:9200                    \
  -v $PWD/testdata:/testdata      \
  $repo_tag

addr=$(docker inspect -f '{{.NetworkSettings.IPAddress}}' $name)
echo "elasticsearch is listening at '$addr:9200'"
