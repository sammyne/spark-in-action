import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {
  // Creates a session on a local master
  val spark = SparkSession.builder
    .appName("Display of shipment")
    .master("local[*]")
    .getOrCreate

  // Reads a JSON, stores it in a dataframe
  // Dataset[Row] == DataFrame
  val df = spark.read
    .format("json")
    .option("multiline", true)
    .load("testdata/shipment.json")

  // Shows at most 5 rows from the dataframe (there's only one anyway)
  df.show(5, 16)
  df.printSchema()
}
