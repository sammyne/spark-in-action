import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main extends App {

  /** The processing code.
    */
  // Create a session on a local master
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Counting the number of meetings per department")
    // To use Databricks Delta Lake, we should add delta core packages to SparkSession
    .config("spark.jars.packages", "io.delta:delta-core_2.13:1.2.1")
    .master("local[*]")
    .getOrCreate()

  val df: Dataset[Row] = spark.read
    .format("delta")
    .load("/tmp/delta_grand_debat_events")

  val df2 = df
    .groupBy(col("authorDept"))
    .count()
    .orderBy(col("count").desc_nulls_first)

  df2.show(25)
  df2.printSchema()

  spark.stop()
}
