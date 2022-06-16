import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions
import scala.collection.mutable.ArrayBuffer

object Main extends App {
  // Create a session on a local master
  val ss = SparkSession.builder
    .appName("Simple SQL")
    .master("local")
    .getOrCreate

  // Create the schema for the whole dataset
  val schema = {
    var fields = new ArrayBuffer[StructField]()

    fields += DataTypes.createStructField("geo", DataTypes.StringType, true)
    for (i <- 1980 to 2010) {
      fields += DataTypes.createStructField(
        "yr%d".format(i),
        DataTypes.DoubleType,
        false,
      )
    }

    DataTypes.createStructType(fields.toArray)
  }

  // Reads a CSV file with header (as specified in the schema), called
  // populationbycountry19802010millions.csv, stores it in a dataframe
  var df = ss.read
    .format("csv")
    .option("header", true)
    .schema(schema)
    .load("testdata/populationbycountry19802010millions.csv")

  for (i <- Range(1981, 2010)) {
    df = df.drop(df.col("yr" + i))
  }

  // Creates a new column with the evolution of the population between
  // 1980 and 2010
  df = df.withColumn(
    "evolution",
    functions.expr("round((yr2010 - yr1980) * 1000000)"),
  )
  df.createOrReplaceTempView("geodata")

  val query1 =
    """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution <= 0
        |ORDER BY evolution
        |LIMIT 25
      """.stripMargin
  val negativeEvolutionDf = ss.sql(query1)

  // Shows at most 15 rows from the dataframe
  negativeEvolutionDf.show(15, false)

  val query2 =
    """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution > 999999
        |ORDER BY evolution DESC
        |LIMIT 25
      """.stripMargin

  val moreThanAMillionDf = ss.sql(query2)
  moreThanAMillionDf.show(15, false)
}
