import org.apache.spark.sql.SparkSession

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark: SparkSession = SparkSession.builder
    .appName("Avro to Dataframe")
    // .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:2.4.5")
    .master("local[*]")
    .getOrCreate

  // Reads an Avro file, stores it in a dataframe
  val df = spark.read
    .format("avro")
    .load("testdata/weather.avro")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema()
  println(s"The dataframe has ${df.count} rows.")
}
