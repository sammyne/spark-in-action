import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark: SparkSession = SparkSession.builder
    .appName("Text to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
  val df = spark.read
    .format("text")
    .load("testdata/romeo-juliet-pg1777.txt")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema
}
