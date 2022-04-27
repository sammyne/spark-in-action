# hello-world

## Quickstart

```bash
sbt clean assembly

docker run --rm               \
  --user root                 \
  -v $PWD:/opt/spark/work-dir \
  -w /opt/spark/work-dir      \
  apache/spark:v3.2.1 bash check.sh
```
