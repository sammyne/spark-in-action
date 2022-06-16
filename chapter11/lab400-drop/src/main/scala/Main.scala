import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

object Main extends App {
  private val logger = LoggerFactory.getLogger(this.getClass())

  // Create a session on a local master
  val ss = SparkSession
    .builder()
    .appName("Simple SQL")
    .master("local")
    .getOrCreate()

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

  logger.info("Territories in orginal dataset: {}", df.count())
  val query =
    """
        |SELECT * FROM geodata
        |WHERE geo is not null and geo != 'Africa'
        | and geo != 'North America' and geo != 'World' and geo != 'Asia & Oceania'
        | and geo != 'Central & South America' and geo != 'Europe' and geo != 'Eurasia'
        | and geo != 'Middle East' order by yr2010 desc
      """.stripMargin

  val cleanedDf = ss.sql(query)

  logger.info("Territories in cleaned dataset: {}", cleanedDf.count())
  cleanedDf.show(20, false)
}
