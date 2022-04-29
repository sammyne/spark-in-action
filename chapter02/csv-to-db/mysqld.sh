#!/bin/bash

name=mysql-spark-in-action
repo_tag=mysql:8.0.28

docker rm -f $name

docker run -td --rm                 \
  --name $name                      \
  -e MYSQL_USER=hello               \
  -e MYSQL_PASSWORD=world           \
  -e MYSQL_ROOT_PASSWORD=helloworld \
  -e MYSQL_DATABASE=spark_labs      \
  $repo_tag

addr=$(docker inspect -f '{{.NetworkSettings.IPAddress}}' $name)
echo "MySQL is listening at '$addr:3306'"
