import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark = SparkSession.builder
    .appName("JSON Lines to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = spark.read
    .format("json")
    .load("testdata/durham-nc-foreclosure-2006-2016.json")

  // Shows at most 5 rows from the dataframe
  df.show(5) // , 13)

  df.printSchema
}
