import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => fn}
import org.apache.spark.sql.DataFrame;
import org.slf4j.LoggerFactory

object Main extends App {
  private val logger = LoggerFactory.getLogger(this.getClass())

  object Mode extends Enumeration {
    val NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER = Value
  }

  val ss = SparkSession
    .builder()
    .appName("Brazil economy")
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setCheckpointDir("/tmp")

  val df = ss.read
    .format("csv")
    .option("header", true)
    .option("sep", ";")
    .option("enforceSchema", true)
    .option("nullValue", "null")
    .option("inferSchema", true)
    .load("testdata/BRAZIL_CITIES.csv")
  println("***** Raw dataset and schema")
  df.show(100)
  df.printSchema()

  val t0 = process(df, Mode.NO_CACHE_NO_CHECKPOINT)
  val t1 = process(df, Mode.CACHE)
  val t2 = process(df, Mode.CHECKPOINT)
  val t3 = process(df, Mode.CHECKPOINT_NON_EAGER)

  println("\n***** Processing times (excluding purification)")
  println("Without cache ............... " + t0 + " ms")
  println("With cache .................. " + t1 + " ms")
  println("With checkpoint ............. " + t2 + " ms")
  println("With non-eager checkpoint ... " + t3 + " ms")

  def process(in: DataFrame, mode: Mode.Value): Long = {
    val t0 = System.currentTimeMillis()

    var df2 = in
      .orderBy(fn.col("CAPITAL").desc)
      .withColumn("WAL-MART", fn.when(fn.col("WAL-MART").isNull, 0).otherwise(fn.col("WAL-MART")))
      .withColumn("MAC", fn.when(fn.col("MAC").isNull, 0).otherwise(fn.col("MAC")))
      .withColumn("GDP", fn.regexp_replace(fn.col("GDP"), ",", "."))
      .withColumn("GDP", fn.col("GDP").cast("float"))
      .withColumn("area", fn.regexp_replace(fn.col("area"), ",", ""))
      .withColumn("area", fn.col("area").cast("float"))
      .groupBy("STATE")
      .agg(
        fn.first("CITY").alias("capital"),
        fn.sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
        fn.sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
        fn.sum("POP_GDP").alias("pop_2016"),
        fn.sum("GDP").alias("gdp_2016"),
        fn.sum("POST_OFFICES").alias("post_offices_ct"),
        fn.sum("WAL-MART").alias("wal_mart_ct"),
        fn.sum("MAC").alias("mc_donalds_ct"),
        fn.sum("Cars").alias("cars_ct"),
        fn.sum("Motorcycles").alias("moto_ct"),
        fn.sum("AREA").alias("area"),
        fn.sum("IBGE_PLANTED_AREA").alias("agr_area"),
        fn.sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
        fn.sum("HOTELS").alias("hotels_ct"),
        fn.sum("BEDS").alias("beds_ct")
      )
      .withColumn("agr_area", fn.expr("agr_area / 100")) // converts hectares
      // to km2
      .orderBy(fn.col("STATE"))
      .withColumn("gdp_capita", fn.expr("gdp_2016 / pop_2016 * 1000"))

    df2 = mode match {
      case Mode.CACHE                  => df2.cache()
      case Mode.CHECKPOINT             => df2.checkpoint()
      case Mode.CHECKPOINT_NON_EAGER   => df2.checkpoint(false)
      case Mode.NO_CACHE_NO_CHECKPOINT => df2
    }

    println("***** Pure data")
    df2.show(5)

    val t1 = System.currentTimeMillis()
    println("Aggregation (ms) .................. " + (t1 - t0))

    println("***** Population")
    val popDf = df2
      .drop(
        "area",
        "pop_brazil",
        "pop_foreign",
        "post_offices_ct",
        "cars_ct",
        "moto_ct",
        "mc_donalds_ct",
        "agr_area",
        "agr_prod",
        "wal_mart_ct",
        "hotels_ct",
        "beds_ct",
        "gdp_capita",
        "agr_area",
        "gdp_2016"
      )
      .orderBy(fn.col("pop_2016").desc)
    popDf.show(30)

    val t2 = System.currentTimeMillis()
    println("Population (ms) ................... " + (t2 - t1))

    println("***** Area (squared kilometers)")
    val areaDf = df2
      .withColumn("area", fn.round(fn.col("area"), 2))
      .drop(
        "pop_2016",
        "pop_brazil",
        "pop_foreign",
        "post_offices_ct",
        "cars_ct",
        "moto_ct",
        "mc_donalds_ct",
        "agr_area",
        "agr_prod",
        "wal_mart_ct",
        "hotels_ct",
        "beds_ct",
        "gdp_capita",
        "agr_area",
        "gdp_2016"
      )
      .orderBy(fn.col("area").desc)

    areaDf.show(30)

    val t3 = System.currentTimeMillis()
    println("Area (ms) ......................... " + (t3 - t2))

    println("***** McDonald's restaurants per 1m inhabitants")
    val mcDonaldsPopDf = df2
      .withColumn("mcd_1m_inh", fn.expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100"))
      .drop(
        "pop_brazil",
        "pop_foreign",
        "post_offices_ct",
        "cars_ct",
        "moto_ct",
        "area",
        "agr_area",
        "agr_prod",
        "wal_mart_ct",
        "hotels_ct",
        "beds_ct",
        "gdp_capita",
        "agr_area",
        "gdp_2016"
      )
      .orderBy(fn.col("mcd_1m_inh").desc)
    mcDonaldsPopDf.show(5)
    val t4 = System.currentTimeMillis()
    println("Mc Donald's (ms) .................. " + (t4 - t3))

    println("***** Walmart supermarket per 1m inhabitants")
    val walmartPopDf = df2
      .withColumn("walmart_1m_inh", fn.expr("int(wal_mart_ct / pop_2016 * 100000000) / 100"))
      .drop(
        "pop_brazil",
        "pop_foreign",
        "post_offices_ct",
        "cars_ct",
        "moto_ct",
        "area",
        "agr_area",
        "agr_prod",
        "mc_donalds_ct",
        "hotels_ct",
        "beds_ct",
        "gdp_capita",
        "agr_area",
        "gdp_2016"
      )
      .orderBy(fn.col("walmart_1m_inh").desc)
    walmartPopDf.show(5);
    val t5 = System.currentTimeMillis()
    println("Walmart (ms) ...................... " + (t5 - t4))

    println("***** GDP per capita")
    val gdpPerCapitaDf = df2
      .drop(
        "pop_brazil",
        "pop_foreign",
        "post_offices_ct",
        "cars_ct",
        "moto_ct",
        "area",
        "agr_area",
        "agr_prod",
        "mc_donalds_ct",
        "hotels_ct",
        "beds_ct",
        "wal_mart_ct",
        "agr_area"
      )
      .withColumn("gdp_capita", fn.expr("int(gdp_capita)"))
      .orderBy(fn.col("gdp_capita").desc)
    gdpPerCapitaDf.show(5)
    val t6 = System.currentTimeMillis()
    println("GDP per capita (ms) ............... " + (t6 - t5))

    println("***** Post offices");
    var postOfficeDf = df2
      .withColumn(
        "post_office_1m_inh",
        fn.expr("int(post_offices_ct / pop_2016 * 100000000) / 100")
      )
      .withColumn("post_office_100k_km2", fn.expr("int(post_offices_ct / area * 10000000) / 100"))
      .drop(
        "gdp_capita",
        "pop_foreign",
        "gdp_2016",
        "gdp_capita",
        "cars_ct",
        "moto_ct",
        "agr_area",
        "agr_prod",
        "mc_donalds_ct",
        "hotels_ct",
        "beds_ct",
        "wal_mart_ct",
        "agr_area",
        "pop_brazil"
      )
      .orderBy(fn.col("post_office_1m_inh").desc)

    postOfficeDf = mode match {
      case Mode.CACHE                  => postOfficeDf.cache()
      case Mode.CHECKPOINT             => postOfficeDf.checkpoint()
      case Mode.CHECKPOINT_NON_EAGER   => postOfficeDf.checkpoint(false)
      case Mode.NO_CACHE_NO_CHECKPOINT => postOfficeDf
    }

    println("****  Per 1 million inhabitants")
    val postOfficePopDf = postOfficeDf
      .drop("post_office_100k_km2", "area")
      .orderBy(fn.col("post_office_1m_inh").desc)
    postOfficePopDf.show(5)
    println("****  per 100000 km2")
    val postOfficeArea = postOfficeDf
      .drop("post_office_1m_inh", "pop_2016")
      .orderBy(fn.col("post_office_100k_km2").desc)
    postOfficeArea.show(5)
    val t7 = System.currentTimeMillis()
    println(
      "Post offices (ms) ................. " + (t7 - t6) + " / Mode: "
        + mode
    )

    println("***** Vehicles")
    val vehiclesDf = df2
      .withColumn("veh_1k_inh", fn.expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100"))
      .drop(
        "gdp_capita",
        "pop_foreign",
        "gdp_2016",
        "gdp_capita",
        "post_offices_ct",
        "agr_area",
        "agr_prod",
        "mc_donalds_ct",
        "hotels_ct",
        "beds_ct",
        "wal_mart_ct",
        "agr_area",
        "area",
        "pop_brazil"
      )
      .orderBy(fn.col("veh_1k_inh").desc)
    vehiclesDf.show(5)
    val t8 = System.currentTimeMillis()
    println("Vehicles (ms) ..................... " + (t8 - t7))

    println("***** Agriculture - usage of land for agriculture")
    val agricultureDf = df2
      .withColumn("agr_area_pct", fn.expr("int(agr_area / area * 1000) / 10"))
      .withColumn("area", fn.expr("int(area)"))
      .drop(
        "gdp_capita",
        "pop_foreign",
        "gdp_2016",
        "gdp_capita",
        "post_offices_ct",
        "moto_ct",
        "cars_ct",
        "mc_donalds_ct",
        "hotels_ct",
        "beds_ct",
        "wal_mart_ct",
        "pop_brazil",
        "agr_prod",
        "pop_2016"
      )
      .orderBy(fn.col("agr_area_pct").desc)
    agricultureDf.show(5)
    val t9 = System.currentTimeMillis()
    println("Agriculture revenue (ms) .......... " + (t9 - t8))

    val end = System.currentTimeMillis()
    println("Total with purification (ms) ...... " + (end - t0))
    println("Total without purification (ms) ... " + (end - t0))

    end - t1
  }
}
