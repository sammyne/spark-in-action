import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Main extends App {
  val s = SparkSession.builder
    .appName("Restaurants in Wake County, NC")
    .master("local")
    .getOrCreate()

  var df = s.read
    .format("json")
    .load("testdata/Restaurants_in_Durham_County_NC.json")

  println("*** Right after ingestion")
  df.show(5)
  df.printSchema()

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

  df = df.withColumn(
    "id",
    functions.concat(
      df.col("state"),
      functions.lit("_"),
      df.col("county"),
      functions.lit("_"),
      df.col("datasetId"),
    ),
  );
  println("*** Dataframe transformed");
  df.show(5)
  df.printSchema()

  System.out.println("*** Looking at partitions")
  println(
    s"Partition count before repartition: ${df.rdd.partitions.length}",
  )

  df = df.repartition(4);
  println(
    s"Partition count after repartition: ${df.rdd.partitions.length}",
  );
}
