# CSV to DB

由于 Elasticsearch 容器读取文件夹权限不足的原因暂时挂起。

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

## 温馨提示
### elasticsearch 启动失败
样例错误如下
```bash
bootstrap check failure [1] of [1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
```
此时需要调整内核参数 `vm.max_map_count` 为 `262144`，执行 `sysctl -w vm.max_map_count=262144` 即可。

## 参考文献
- [Using the Elastic Stack to Analyze NYC Restaurant Inspection Data](https://github.com/elastic/examples/blob/master/Exploring%20Public%20Datasets/nyc_restaurants/README.md)
