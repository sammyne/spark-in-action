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
[info] compiling 1 Scala source to /github.com/sammyne/spark-in-action/chapter08/lab300-advanced-queries/target/scala-2.13/classes ...
[warn] 3 deprecations (since 2.13.3); re-run with -deprecation for details
[warn] one warning found
[info] running (fork) Main 192.168.10.10:3306
[info] +-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
[info] |film_id|         title|         description|release_year|language_id|original_language_id|rental_duration|rental_rate|length|replacement_cost|rating|    special_features|        last_update|
[info] +-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
[info] |      6|  AGENT TRUMAN|A Intrepid Panora...|  2006-01-01|          1|                null|              3|       2.99|   169|           17.99|    PG|      Deleted Scenes|2006-02-15 05:03:42|
[info] |     13|   ALI FOREVER|A Action-Packed D...|  2006-01-01|          1|                null|              4|       4.99|   150|           21.99|    PG|Deleted Scenes,Be...|2006-02-15 05:03:42|
[info] |    137|CHARADE DUFFEL|A Action-Packed D...|  2006-01-01|          1|                null|              3|       2.99|    66|           21.99|    PG|Trailers,Deleted ...|2006-02-15 05:03:42|
[info] |    217|    DAZED PUNK|A Action-Packed S...|  2006-01-01|          1|                null|              6|       4.99|   120|           20.99|     G|Commentaries,Dele...|2006-02-15 05:03:42|
[info] |    396|  HANGING DEEP|A Action-Packed Y...|  2006-01-01|          1|                null|              5|       4.99|    62|           18.99|     G|Trailers,Commenta...|2006-02-15 05:03:42|
[info] +-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
[info] only showing top 5 rows
[info] root
[info]  |-- film_id: integer (nullable = true)
[info]  |-- title: string (nullable = true)
[info]  |-- description: string (nullable = true)
[info]  |-- release_year: date (nullable = true)
[info]  |-- language_id: integer (nullable = true)
[info]  |-- original_language_id: integer (nullable = true)
[info]  |-- rental_duration: integer (nullable = true)
[info]  |-- rental_rate: decimal(4,2) (nullable = true)
[info]  |-- length: integer (nullable = true)
[info]  |-- replacement_cost: decimal(5,2) (nullable = true)
[info]  |-- rating: string (nullable = true)
[info]  |-- special_features: string (nullable = true)
[info]  |-- last_update: timestamp (nullable = true)
[info] The dataframe contains 16 record(s).
[success] Total time: 11 s, completed May 17, 2022 1:39:04 AM
```

### 3. 清理资源
```bash
docker rm -f mysql-spark-inaction
```
