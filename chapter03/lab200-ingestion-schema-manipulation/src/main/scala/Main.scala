import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Main extends App {
  val s = SparkSession.builder
    .appName("Restaurants in Wake County, NC")
    .master("local")
    .getOrCreate()

  var df = s.read
    .format("csv")
    .option("header", "true")
    .load("testdata/Restaurants_in_Wake_County_NC.csv")

  println("*** Right after ingestion")
  println(s"We have ${df.count()} records")
  df.printSchema()
  df.show(5)

  // Let's transform our dataframe
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
    .withColumnRenamed("FACILITYTYPE", "type")
    .withColumnRenamed("X", "geoX")
    .withColumnRenamed("Y", "geoY")
    .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

  df = df.withColumn(
    "id",
    functions.concat(
      df.col("state"),
      functions.lit("_"),
      df.col("county"),
      functions.lit("_"),
      df.col("datasetId"),
    ),
  )

  println("*** Dataframe transformed")
  df.show(5)
  df.printSchema()

  println("*** Looking at partitions")
  val partitions = df.rdd.partitions
  println(s"Partition count before repartition: ${partitions.length}")

  df = df.repartition(4)
  println(s"Partition count after repartition: ${df.rdd.partitions.length}")
}
