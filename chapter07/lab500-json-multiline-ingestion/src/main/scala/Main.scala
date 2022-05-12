import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark = SparkSession.builder
    .appName("Multiline JSON to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
  val df = spark.read
    .format("json")
    .option("multiline", true)
    .load("testdata/countrytravelinfo.json")

  // Shows at most 3 rows from the dataframe
  df.show(3)
  df.printSchema()
}
