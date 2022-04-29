import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import java.util.Properties
import org.apache.spark.sql.SaveMode

object Main extends App {
  if (this.args.isEmpty) {
    throw new IllegalArgumentException("miss mysql address")
  }
  val mysqlAddr = this.args(0)
  println(s"using MySQL at $mysqlAddr")

  val s =
    SparkSession.builder.appName("CSV to DB").master("local[*]").getOrCreate()

  var df =
    s.read.format("csv").option("header", true).load("testdata/authors.csv")

  df = df.withColumn(
    "name",
    functions.concat(
      functions.col("lname"),
      functions.lit(", "),
      functions.col("fname"),
    ),
  )

  val dbUrl = s"jdbc:mysql://$mysqlAddr/spark_labs"

  val prop = new Properties
  // Loading class `com.mysql.jdbc.Driver'. This is deprecated. The new driver class is
  // `com.mysql.cj.jdbc.Driver'. The driver is automatically registered via the SPI and
  // manual loading of the driver class is generally unnecessary.
  // prop.setProperty("driver", "com.mysql.jdbc.Driver")
  prop.setProperty("user", "hello")
  prop.setProperty("password", "world")

  df.write.mode(SaveMode.Overwrite).jdbc(dbUrl, "chapter02", prop)

  s.stop

  println("done")
}
