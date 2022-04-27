import org.apache.spark.sql.SparkSession

object Main {
  // Note that applications should define a main() method instead of extending scala.App.
  // Subclasses of scala.App may not work correctly.
  def main(args: Array[String]) {
    val s =
      SparkSession.builder
        .appName("CSV to Dataset")
        .master("local")
        .getOrCreate()

    val df =
      s.read.format("csv").option("header", "true").load("testdata/books.csv")

    df.show(5)

    s.stop()
  }
}
