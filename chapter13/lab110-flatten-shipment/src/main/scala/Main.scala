import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Main extends App {

  /** The processing code.
    */
  // Creates a session on a local master
  val spark = SparkSession.builder
    .appName("Flatenning JSON doc describing shipments")
    .master("local[*]")
    .getOrCreate

  // Reads a JSON, stores it in a dataframe
  // Dataset[Row] == DataFrame
  val df = spark.read
    .format("json")
    .option("multiline", true)
    .load("testdata/shipment.json")

  val df2 = df
    .withColumn("supplier_name", functions.col("supplier.name"))
    .withColumn("supplier_city", functions.col("supplier.city"))
    .withColumn("supplier_state", functions.col("supplier.state"))
    .withColumn("supplier_country", functions.col("supplier.country"))
    .drop("supplier")
    .withColumn("customer_name", functions.col("customer.name"))
    .withColumn("customer_city", functions.col("customer.city"))
    .withColumn("customer_state", functions.col("customer.state"))
    .withColumn("customer_country", functions.col("customer.country"))
    .drop("customer")
    .withColumn("items", functions.explode(functions.col("books")))

  val df3 = df2
    .withColumn("qty", functions.col("items.qty"))
    .withColumn("title", functions.col("items.title"))
    .drop("items")
    .drop("books")

  // Shows at most 5 rows from the dataframe (there's only one anyway)
  df3.show(5, false)
  df3.printSchema()

  df3.createOrReplaceTempView("shipment_detail")

  val sqlQuery = "SELECT COUNT(*) AS bookCount FROM shipment_detail"
  val bookCountDf = spark.sql(sqlQuery)

  bookCountDf.show
}
