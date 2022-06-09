import org.apache.spark.sql.SparkSession

object Main extends App {
  // Get a session
  val spark = SparkSession.builder
    .appName("EXIF to Dataset")
    .master("local[1]")
    .getOrCreate

  // read the data
  val df = spark.read
    .format("x.ds.exif")
    .option("recursive", "true")
    .option("limit", "100000")
    .option("extensions", "jpg,jpeg")
    .load("testdata")

  println(s"I have imported ${df.count} photos.")
  df.printSchema()
  df.show(5)
}
