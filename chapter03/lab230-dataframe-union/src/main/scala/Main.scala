import org.apache.spark.sql.{SparkSession, Row, Dataset}
import org.apache.spark.sql.functions

object Main extends App {
  val s = SparkSession.builder
    .appName("Restaurants in Wake County, NC")
    .master("local")
    .getOrCreate()

  val df1 = buildDurhamRestaurantsDataframe(s)
  val df2 = buildWakeRestaurantsDataframe(s)

  combineDataframes(df1, df2)

  /** Builds the dataframe containing the Durham county restaurants
    *
    * @return
    *   A dataframe
    */
  private def buildDurhamRestaurantsDataframe(s: SparkSession): Dataset[Row] = {
    var df = s.read
      .format("json")
      .load("testdata/Restaurants_in_Durham_County_NC.json")

    val drop_cols = List("fields", "geometry", "record_timestamp", "recordid")
    df = df
      .withColumn("county", functions.lit("Durham"))
      .withColumn("datasetId", functions.col("fields.id"))
      .withColumn("name", functions.col("fields.premise_name"))
      .withColumn("address1", functions.col("fields.premise_address1"))
      .withColumn("address2", functions.col("fields.premise_address2"))
      .withColumn("city", functions.col("fields.premise_city"))
      .withColumn("state", functions.col("fields.premise_state"))
      .withColumn("zip", functions.col("fields.premise_zip"))
      .withColumn("tel", functions.col("fields.premise_phone"))
      .withColumn("dateStart", functions.col("fields.opening_date"))
      .withColumn("dateEnd", functions.col("fields.closing_date"))
      .withColumn(
        "type",
        functions
          .split(functions.col("fields.type_description"), " - ")
          .getItem(1),
      )
      .withColumn("geoX", functions.col("fields.geolocation").getItem(0))
      .withColumn("geoY", functions.col("fields.geolocation").getItem(1))
      .drop(drop_cols: _*)

    df = df.withColumn(
      "id",
      functions.concat(
        functions.col("state"),
        functions.lit("_"),
        functions.col("county"),
        functions.lit("_"),
        functions.col("datasetId"),
      ),
    )
    // I left the following line if you want to play with repartitioning
    // df1 = df1.repartition(4);
    df
  }

  private def buildWakeRestaurantsDataframe(s: SparkSession): Dataset[Row] = {
    var df = s.read
      .format("csv")
      .option("header", "true")
      .load("testdata/Restaurants_in_Wake_County_NC.csv")

    val drop_cols = List("OBJECTID", "GEOCODESTATUS", "PERMITID")
    df = df
      .withColumn("county", functions.lit("Wake"))
      .withColumnRenamed("HSISID", "datasetId")
      .withColumnRenamed("NAME", "name")
      .withColumnRenamed("ADDRESS1", "address1")
      .withColumnRenamed("ADDRESS2", "address2")
      .withColumnRenamed("CITY", "city")
      .withColumnRenamed("STATE", "state")
      .withColumnRenamed("POSTALCODE", "zip")
      .withColumnRenamed("PHONENUMBER", "tel")
      .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
      .withColumn("dateEnd", functions.lit(null))
      .withColumnRenamed("FACILITYTYPE", "type")
      .withColumnRenamed("X", "geoX")
      .withColumnRenamed("Y", "geoY")
      .drop(drop_cols: _*)

    df = df.withColumn(
      "id",
      functions.concat(
        functions.col("state"),
        functions.lit("_"),
        functions.col("county"),
        functions.lit("_"),
        functions.col("datasetId"),
      ),
    )
    // I left the following line if you want to play with repartitioning
    // df1 = df1.repartition(4);
    df
  }

  private def combineDataframes(df1: Dataset[Row], df2: Dataset[Row]): Unit = {
    val df = df1.unionByName(df2)
    df.show(5)
    df.printSchema()
    println(s"We have ${df.count} records.")
    println(s"Partition count: ${df.rdd.getNumPartitions}")
  }
}
