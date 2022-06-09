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

### 3. 清理资源
```bash
docker rm -f mysql-spark-inaction
```
