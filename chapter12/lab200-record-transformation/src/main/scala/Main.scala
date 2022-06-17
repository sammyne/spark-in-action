import org.apache.spark.sql.{SparkSession, functions => fn}

object Main extends App {

  /** The processing code.
    */
  // Creation of the session
  val spark: SparkSession = SparkSession.builder
    .appName("Record transformations")
    .master("local[*]")
    .getOrCreate

  // Ingestion of the census data
  var intermediateDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("testdata/census/PEP_2017_PEPANNRES.csv")

  // Renaming and dropping the columns we do not need
  intermediateDf = intermediateDf
    .drop("GEO.id")
    .withColumnRenamed("GEO.id2", "id")
    .withColumnRenamed("GEO.display-label", "label")
    .withColumnRenamed("rescen42010", "real2010")
    .drop("resbase42010")
    .withColumnRenamed("respop72010", "est2010")
    .withColumnRenamed("respop72011", "est2011")
    .withColumnRenamed("respop72012", "est2012")
    .withColumnRenamed("respop72013", "est2013")
    .withColumnRenamed("respop72014", "est2014")
    .withColumnRenamed("respop72015", "est2015")
    .withColumnRenamed("respop72016", "est2016")
    .withColumnRenamed("respop72017", "est2017")

  intermediateDf.printSchema()
  intermediateDf.show(5)

  // Creates the additional columns
  intermediateDf = intermediateDf
    .withColumn("countyState", fn.split(fn.col("label"), ", "))
    .withColumn("stateId", fn.expr("int(id/1000)"))
    .withColumn("countyId", fn.expr("id%1000"))

  intermediateDf.printSchema()
  intermediateDf.sample(.01).show(5, false)

  intermediateDf = intermediateDf
    .withColumn("state", fn.col("countyState").getItem(1))
    .withColumn("county", fn.col("countyState").getItem(0))
    .drop("countyState")

  intermediateDf.printSchema()
  intermediateDf.sample(.01).show(5, false)

  // I could split the column in one operation if I wanted:
  // val countyStateDf: Dataset[Row] = intermediateDf
  //  .withColumn("state", fn.split(fn.col("label"), ", ").getItem(1))
  //  .withColumn("county", fn.split(fn.col("label"), ", ").getItem(0))

  // Performs some statistics on the intermediate dataframe
  var statDf = intermediateDf
    .withColumn("diff", fn.expr("est2010-real2010"))
    .withColumn("growth", fn.expr("est2017-est2010"))
    .drop("id")
    .drop("label")
    .drop("real2010")
    .drop("est2010")
    .drop("est2011")
    .drop("est2012")
    .drop("est2013")
    .drop("est2014")
    .drop("est2015")
    .drop("est2016")
    .drop("est2017")

  statDf.printSchema()
  statDf.sample(.01).show(5, false)

  // Extras: see how you can sort!
  statDf = statDf.sort(fn.col("growth").desc)
  println("Top 5 counties with the most growth:")
  statDf.show(5, false)
  //
  // statDf = statDf.sort(fn.col("growth"))
  // println("Top 5 counties with the most loss:")
  // statDf.show(5, false)
}
