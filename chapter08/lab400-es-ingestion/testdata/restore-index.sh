#!/bin/bash

set -e

#password=$(docker logs es-spark-in-action | grep -A1 'Password for' | tail -1)
#auth_opt="--user elastic:$password"
es_addr=localhost:9200

curl $auth_opt -k -H "Content-Type: application/json" -XPUT "http://$es_addr/_snapshot/restaurants_backup" -d '{
    "type": "fs",
    "settings": {
        "location": "/testdata/nyc_restaurants",
        "compress": true,
        "max_snapshot_bytes_per_sec": "1000mb",
        "max_restore_bytes_per_sec": "1000mb"
    }
}'

echo "hello --------"
curl $auth_opt -k -XPOST "http://$es_addr/_snapshot/restaurants_backup/snapshot_1/_restore"
