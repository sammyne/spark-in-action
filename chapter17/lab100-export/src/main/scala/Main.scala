import java.io.{FileOutputStream, IOException}
import java.net.URL
import java.nio.channels.Channels

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object Main extends App {

  val modisFile = "testdata/MODIS_C6_1_Global_24h.csv"
  val viirsFile = "testdata/SUOMI_VIIRS_C2_Global_24h.csv"

  private val logger = LoggerFactory.getLogger(this.getClass())

  val spark = SparkSession
    .builder()
    .appName("Wildfire data pipeline")
    .master("local[*]")
    .getOrCreate()

  // Format the VIIRS dataset
  val viirsDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(viirsFile)

  val viirsDf2 = viirsDf
    .withColumn("acq_time_min", expr("acq_time % 100"))
    .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
    .withColumn("acq_time2", unix_timestamp(col("acq_date")))
    .withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
    .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
    .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3")
    .withColumnRenamed("confidence", "confidence_level")
    .withColumn("brightness", lit(null))
    .withColumn("bright_t31", lit(null))

  viirsDf2.show()
  viirsDf2.printSchema()

  // This piece of code shows the repartition by confidence level, so you
  // can compare when you convert the confidence as a % to a level for the
  // MODIS dataset.
  var df = viirsDf2.groupBy("confidence_level").count()
  val count = viirsDf2.count()
  df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2))
  df.show()

  // Format the MODIS dataset
  val low = 40
  val high = 100

  val modisDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(modisFile)
    .withColumn("acq_time_min", expr("acq_time % 100"))
    .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
    .withColumn("acq_time2", unix_timestamp(col("acq_date")))
    .withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
    .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
    .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3")
    .withColumn("confidence_level", when(col("confidence").$less$eq(low), "low"))
    .withColumn(
      "confidence_level",
      when(col("confidence").$greater(low).and(col("confidence").$less(high)), "nominal")
        .otherwise(col("confidence_level"))
    )
    .withColumn(
      "confidence_level",
      when(isnull(col("confidence_level")), "high")
        .otherwise(col("confidence_level"))
    )
    .drop("confidence")
    .withColumn("bright_ti4", lit(null))
    .withColumn("bright_ti5", lit(null))

  modisDf.show()
  modisDf.printSchema()

  // This piece of code shows the repartition by confidence level, so you
  // can compare when you convert the confidence as a % to a level for the
  // MODIS dataset.
  df = modisDf.groupBy("confidence_level").count()
  val count2 = modisDf.count()
  df = df.withColumn("%", round(expr("100 / " + count2 + " * count"), 2))
  df.show()

  val wildfireDf = viirsDf2.unionByName(modisDf)
  wildfireDf.show()
  wildfireDf.printSchema()

  logger.info(s"# of partitions: ${wildfireDf.rdd.getNumPartitions}")

  wildfireDf.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("/tmp/fires_parquet")

  val outputDf = wildfireDf
    .filter("confidence_level = 'high'")
    .repartition(1)

  outputDf.write
    .format("csv")
    .option("header", true)
    .mode(SaveMode.Overwrite)
    .save("/tmp/high_confidence_fires_csv")

  /** Downloads data files to local temp value.
    *
    * @param fromFile
    * @param toFile
    * @return
    */
  private def download(fromFile: String, toFile: String): Boolean = {
    try {
      val website = new URL(fromFile)
      val rbc = Channels.newChannel(website.openStream)
      val fos = new FileOutputStream(toFile)
      fos.getChannel.transferFrom(rbc, 0, Long.MaxValue)
      fos.close()
      rbc.close()
    } catch {
      case e: IOException =>
        logger.debug("Error while downloading '{}', got: {}", fromFile, e.getMessage, e)
        return false
    }
    logger.debug("{} downloaded successfully.", toFile)
    true
  }

}
