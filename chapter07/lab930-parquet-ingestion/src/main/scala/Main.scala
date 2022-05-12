import org.apache.spark.sql.SparkSession

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark: SparkSession = SparkSession.builder
    .appName("Parquet to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a Parquet file, stores it in a dataframe
  val df = spark.read
    .format("parquet")
    .load("testdata/alltypes_plain.parquet")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema()
  println(s"The dataframe has ${df.count} rows.")
}
