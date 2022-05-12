import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark: SparkSession = SparkSession.builder
    .appName("ORC to Dataframe")
    .config("spark.sql.orc.impl", "native")
    .master("local[*]")
    .getOrCreate

  // Reads an ORC file, stores it in a dataframe
  val df = spark.read
    .format("orc")
    .load("testdata/demo-11-zlib.orc")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema()

  println(s"The dataframe has ${df.count} rows.")
}
