import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField

object Main extends App {
  val ss = SparkSession
    .builder()
    .appName("Simple SELECT using SQL")
    .master("local")
    .getOrCreate()

  val schema = DataTypes.createStructType(
    Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
    ),
  )

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = ss.read
    .format("csv")
    .option("header", true)
    .schema(schema)
    .load("testdata/populationbycountry19802010millions.csv")

  df.createOrReplaceTempView("geodata")
  df.printSchema()

  val query =
    """
        |SELECT * FROM geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin

  val smallCountries = ss.sql(query)

  // Shows at most 10 rows from the dataframe (which is limited to 5
  // anyway)
  smallCountries.show(10, false)
}
