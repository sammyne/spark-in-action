#!/bin/bash

cd $(dirname $0)

name=mysql-spark-in-action
repo_tag=mysql:8.0.28

docker rm -f $name

docker run -td --rm                 \
  --name $name                      \
  -e MYSQL_USER=hello               \
  -e MYSQL_PASSWORD=world           \
  -e MYSQL_ROOT_PASSWORD=helloworld \
  -e MYSQL_DATABASE=sakila          \
  -v $PWD/testdata:/testdata        \
  $repo_tag 

#exit 0

while true; do
  echo "waiting mysqld to be ready ..."

  ok=$(docker exec $name bash -c "[ -e /var/run/mysqld/mysqld.sock ] && echo 'yes'")
  if [[ -n "$ok" ]]; then
    break
  fi
  sleep 8s
done
echo "mysqld is up :)"

# wait mysqld to be ready
sleep 10s

set -e

echo "set up sakila database"
docker exec $name sh -c "mysql -hlocalhost -uroot -phelloworld < /testdata/sakila-schema.sql"
docker exec $name sh -c "mysql -hlocalhost -uroot -phelloworld < /testdata/sakila-data.sql"

addr=$(docker inspect -f '{{.NetworkSettings.IPAddress}}' $name)
echo "MySQL is listening at '$addr:3306'"
