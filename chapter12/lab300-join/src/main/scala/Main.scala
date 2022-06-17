import org.apache.spark.sql.{SparkSession, functions => fn}

object Main extends App {

  /** The processing code.
    */
  // Creation of the session
  val spark: SparkSession = SparkSession.builder
    .appName("Join")
    .master("local[*]")
    .getOrCreate

  // Ingestion of the census data
  var censusDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "cp1252")
    .load("testdata/census/PEP_2017_PEPANNRES.csv")

  censusDf = censusDf
    .drop("GEO.id")
    .drop("rescen42010")
    .drop("resbase42010")
    .drop("respop72010")
    .drop("respop72011")
    .drop("respop72012")
    .drop("respop72013")
    .drop("respop72014")
    .drop("respop72015")
    .drop("respop72016")
    .withColumnRenamed("respop72017", "pop2017")
    .withColumnRenamed("GEO.id2", "countyId")
    .withColumnRenamed("GEO.display-label", "county")

  println("Census data")
  censusDf.sample(0.1).show(3, false)
  censusDf.printSchema()

  // Higher education institution (and yes, there is an Arkansas College
  // of Barbering and Hair Design)
  var higherEdDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("testdata/dapip/InstitutionCampus.csv")

  higherEdDf = higherEdDf
    .filter("LocationType = 'Institution'")
    .withColumn("addressElements", fn.split(fn.col("Address"), " "))

  higherEdDf = higherEdDf.withColumn(
    "addressElementCount",
    fn.size(fn.col("addressElements")),
  )

  higherEdDf = higherEdDf
    .withColumn(
      "zip9",
      fn.element_at(fn.col("addressElements"), fn.col("addressElementCount")),
    )

  higherEdDf = higherEdDf
    .withColumn("splitZipCode", fn.split(fn.col("zip9"), "-"))

  higherEdDf = higherEdDf
    .withColumn("zip", fn.col("splitZipCode").getItem(0))
    .withColumnRenamed("LocationName", "location")
    .drop("DapipId")
    .drop("OpeId")
    .drop("ParentName")
    .drop("ParentDapipId")
    .drop("LocationType")
    .drop("Address")
    .drop("GeneralPhone")
    .drop("AdminName")
    .drop("AdminPhone")
    .drop("AdminEmail")
    .drop("Fax")
    .drop("UpdateDate")
    .drop("zip9")
    .drop("addressElements")
    .drop("addressElementCount")
    .drop("splitZipCode")

  println("Higher education institutions (DAPIP)")
  higherEdDf.sample(0.1).show(3, false)
  higherEdDf.printSchema()

  // Zip to county
  var countyZipDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("testdata/hud/COUNTY_ZIP_092018.csv")

  countyZipDf = countyZipDf
    .drop("res_ratio")
    .drop("bus_ratio")
    .drop("oth_ratio")
    .drop("tot_ratio")

  println("Counties / ZIP Codes (HUD)")
  countyZipDf.sample(0.1).show(3, false)
  countyZipDf.printSchema()

  val institPerCountyJoinCondition =
    higherEdDf.col("zip") === countyZipDf.col("zip")
  // Institutions per county id
  var institPerCountyDf = higherEdDf
    .join(countyZipDf, institPerCountyJoinCondition, "inner")
    .drop(countyZipDf.col("zip"))

  println("Higher education institutions left-joined with HUD")
  institPerCountyDf.filter(fn.col("zip") === 27517).show(20, false)
  institPerCountyDf.printSchema()

  // --------------------------
  // - "Temporary" drop columns
  // Note:
  // This block is not doing anything except illustrating that the drop()
  // method needs to be used carefully.
  // Dropping all zip columns
  println("Attempt to drop the zip column")
  institPerCountyDf
    .drop("zip")
    .sample(0.1)
    .show(3, false)

  // Dropping the zip column inherited from the higher ed dataframe
  println("Attempt to drop the zip column")
  institPerCountyDf
    .drop(fn.col("zip"))
    .sample(0.1)
    .show(3, false)
  // --------------------------

  val institPerCountyCondition =
    institPerCountyDf.col("county") === censusDf.col("countyId")
  // Institutions per county name
  institPerCountyDf = institPerCountyDf
    .join(censusDf, institPerCountyCondition, "left")
    .drop(censusDf.col("county"))

  // Final clean up
  institPerCountyDf = institPerCountyDf
    .drop(fn.col("zip"))
    .drop(fn.col("county"))
    .drop("countyId")
    .distinct

  println("Higher education institutions in ZIP Code 27517 (NC)")
  institPerCountyDf.filter(fn.col("zip") === 27517).show(20, false)

  println("Higher education institutions in ZIP Code 02138 (MA)")
  institPerCountyDf.filter(higherEdDf.col("zip") === 2138).show(20, false)

  println("Institutions with improper counties")
  institPerCountyDf.filter("county is null").show(200, false)

  println("Final list")
  institPerCountyDf.show(200, false)
  println("The combined list has " + institPerCountyDf.count + " elements.")

  // A little more
  // var aggDf = institPerCountyDf.groupBy("county", "pop2017").count
  // aggDf = aggDf.orderBy(aggDf.col("count").desc)
  // aggDf.show(25, false)
  //
  // var popDf = aggDf.filter("pop2017>30000")
  //   .withColumn("institutionPer10k", fn.expr("count*10000/pop2017"))
  //
  // popDf = popDf.orderBy(popDf.col("institutionPer10k").desc)
  // popDf.show(25, false)
}
