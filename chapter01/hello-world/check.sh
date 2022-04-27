#!/bin/bash

echo "--- start spark master ..."
bash $SPARK_HOME/sbin/start-master.sh

echo "--- submit job ..."
$SPARK_HOME/bin/spark-submit --class Main target/scala-2.13/hello-world-assembly-1.0.jar
