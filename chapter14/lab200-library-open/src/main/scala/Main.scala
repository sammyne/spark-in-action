import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{
  Dataset,
  Row,
  RowFactory,
  SparkSession,
  functions => fn,
}

/** Custom UDF to check if in range.
  *
  * @author
  *   rambabu.posa
  */
object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark: SparkSession = SparkSession.builder
    .appName("Custom UDF to check if in range")
    .master("local[*]")
    .getOrCreate

  spark.udf.register("isOpen", new IsOpenUdf, DataTypes.BooleanType)

  val librariesDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("encoding", "cp1252")
    .load("testdata/sdlibraries.csv")
    .drop("Administrative_Authority")
    .drop("Address1")
    .drop("Address2")
    .drop("Town")
    .drop("Postcode")
    .drop("County")
    .drop("Phone")
    .drop("Email")
    .drop("Website")
    .drop("Image")
    .drop("WGS84_Latitude")
    .drop("WGS84_Longitude")

  librariesDf.show(false)
  librariesDf.printSchema()

  val dateTimeDf = createDataframe(spark)
  dateTimeDf.show(false)
  dateTimeDf.printSchema()

  val df = librariesDf.crossJoin(dateTimeDf)
  df.show(false)

  // Using the dataframe API
  val finalDf = df
    .withColumn(
      "open",
      fn.callUDF(
        "isOpen",
        fn.col("Opening_Hours_Monday"),
        fn.col("Opening_Hours_Tuesday"),
        fn.col("Opening_Hours_Wednesday"),
        fn.col("Opening_Hours_Thursday"),
        fn.col("Opening_Hours_Friday"),
        fn.col("Opening_Hours_Saturday"),
        fn.lit("Closed"),
        fn.col("date"),
      ),
    )
    .drop("Opening_Hours_Monday")
    .drop("Opening_Hours_Tuesday")
    .drop("Opening_Hours_Wednesday")
    .drop("Opening_Hours_Thursday")
    .drop("Opening_Hours_Friday")
    .drop("Opening_Hours_Saturday")

  finalDf.show()

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(
      Array[StructField](
        DataTypes.createStructField("date_str", DataTypes.StringType, false),
      ),
    )

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("2019-03-11 14:30:00"))
    rows.add(RowFactory.create("2019-04-27 16:00:00"))
    rows.add(RowFactory.create("2020-01-26 05:00:00"))

    spark
      .createDataFrame(rows, schema)
      .withColumn("date", fn.to_timestamp(fn.col("date_str")))
      .drop("date_str")
  }
}
