# CSV to DB

## 快速开始

### 1. 启动 MySQL 服务
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

### 3. 查看数据库表，确认数据写入成功
```bash
#  mysqld 脚本启动的容器名称
docker exec mysql-spark-in-action \
  mysql -uhello -pworld spark_labs -Bse 'select * from chapter02;'
```

数据插入情况下，可得样例日志如下

```bash
Pascal  Blaise  Pascal, Blaise
Voltaire        Fran�ois        Voltaire, Fran�ois
Perrin  Jean-Georges    Perrin, Jean-Georges
Mar�chal        Pierre Sylvain  Mar�chal, Pierre Sylvain
Karau   Holden  Karau, Holden
Zaharia Matei   Zaharia, Matei
```

### 4. 清理资源
```bash
docker rm -f mysql-spark-inaction
```
