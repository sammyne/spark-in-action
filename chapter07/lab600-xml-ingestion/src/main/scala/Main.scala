import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark: SparkSession = SparkSession.builder
    .appName("XML to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "row")
    .load("testdata/nasa-patents.xml")

  // Shows at most 5 rows from the dataframe
  df.show(5)
  df.printSchema

}
