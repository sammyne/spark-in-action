# Spark in Action

此仓库实现 Spark in Action 第 2 版的样例程序。

## 环境
- Scala 2.13.8
- OpenJDK 1.8.0_312
- Spark 3.2.1
- sbt 1.6.2

为了简化开发，可直接使用这个 docker 镜像 sammyne/scala:2.13.8-jdk8-ubuntu20.04。

## 注意事项
- 使用 OpenJDK 11 会触发以下警告，不影响运行
    ```bash
    [error] WARNING: An illegal reflective access operation has occurred
    [error] WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/github.com/sammyne/spark-in-action/chapter01/hello-world/target/bg-jobs/sbt_23c23a4b/target/e6cc3437/08fe7b74/spark-unsafe_2.13-3.2.1.jar) to constructor java.nio.DirectByteBuffer(long,int)
    [error] WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
    [error] WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    [error] WARNING: All illegal access operations will be denied in a future release
    ```
     参考 StackOverflow [回答](https://stackoverflow.com/a/65463944/10878967)。
- sbt 有 bug，导致 `sbt run` 出错 

    为此，需要再 build.sbt 文件添加配置 `run / fork := true`。添加之后项目运行过程还会输出以下 `[error]` 前缀的日志，

    ```bash
    [error] Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    [error] 22/04/28 03:31:09 INFO SparkContext: Running Spark version 3.2.1
    ```
    目前看起来不影响程序正确性，其中 `[error]` 前缀是 `sbt` 添加的，直接运行 `sbt assembly` 打包的 jar 不会输出 `[error]` 前缀。 