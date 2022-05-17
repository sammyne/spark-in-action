# CSV to DB

## 快速开始

### 1. 启动 MySQL 服务并导入数据
```bash
bash mysqld.sh
```

样例输出如下

```bash
MySQL is listening at '192.168.10.10:3306'
```

表示 mysqld 进程的监听地址为 `192.168.10.10:3306`。

### 2. 运行示例程序
```bash
# run 的参数指定为上一步输出的 mysqld 监听地址
sbt "run 192.168.10.10:3306"
```

成功运行的样例输出如下

```bash
[info] compiling 1 Scala source to /github.com/sammyne/spark-in-action/chapter08/lab100-mysql-ingestion/target/scala-2.13/classes ...
[warn] 3 deprecations (since 2.13.3); re-run with -deprecation for details
[warn] one warning found
[info] running (fork) Main 192.168.10.10:3306
[info] +--------+----------+---------+-------------------+
[info] |actor_id|first_name|last_name|        last_update|
[info] +--------+----------+---------+-------------------+
[info] |      92|   KIRSTEN|   AKROYD|2006-02-15 04:34:33|
[info] |      58| CHRISTIAN|   AKROYD|2006-02-15 04:34:33|
[info] |     182|    DEBBIE|   AKROYD|2006-02-15 04:34:33|
[info] |     118|      CUBA|    ALLEN|2006-02-15 04:34:33|
[info] |     145|       KIM|    ALLEN|2006-02-15 04:34:33|
[info] +--------+----------+---------+-------------------+
[info] only showing top 5 rows
[info] root
[info]  |-- actor_id: integer (nullable = true)
[info]  |-- first_name: string (nullable = true)
[info]  |-- last_name: string (nullable = true)
[info]  |-- last_update: timestamp (nullable = true)
[info] The dataframe contains 200 record(s).
[success] Total time: 7 s, completed May 17, 2022 12:59:16 AM
```

### 3. 清理资源
```bash
docker rm -f mysql-spark-inaction
```
