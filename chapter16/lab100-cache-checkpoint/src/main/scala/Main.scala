import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => fn}
import org.apache.spark.sql.functions.{callUDF, col, sum, when}

object Main extends App {
  object Mode extends Enumeration {
    val NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER = Value
  }

  val ss = SparkSession
    .builder()
    .appName("Example of cache and checkpoint")
    .master("local[*]")
    .config("spark.executor.memory", "70g")
    .config("spark.driver.memory", "50g")
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", "16g")
    .getOrCreate()

  ss.sparkContext.setCheckpointDir("/tmp")

  // Specify the number of records to generate
  val recordCount = 1000

  // Create and process the records without cache or checkpoint
  val t0 = processDataframe(ss, recordCount, Mode.NO_CACHE_NO_CHECKPOINT);

  // Create and process the records with cache
  val t1 = processDataframe(ss, recordCount, Mode.CACHE);

  // Create and process the records with a checkpoint
  val t2 = processDataframe(ss, recordCount, Mode.CHECKPOINT);

  // Create and process the records with a checkpoint
  val t3 = processDataframe(ss, recordCount, Mode.CHECKPOINT_NON_EAGER);

  println("\nProcessing times");
  println("Without cache ............... " + t0 + " ms")
  println("With cache .................. " + t1 + " ms")
  println("With checkpoint ............. " + t2 + " ms")
  println("With non-eager checkpoint ... " + t3 + " ms")

  private def processDataframe(ss: SparkSession, recordCount: Int, mode: Mode.Value): Long = {
    val df = RecordGenerator.createDataframe(ss, recordCount)

    val t0 = System.currentTimeMillis()
    var topDf = df.filter(col("rating").equalTo(5));
    mode match {
      case Mode.CACHE                  => topDf = topDf.cache()
      case Mode.CHECKPOINT             => topDf = topDf.checkpoint()
      case Mode.CHECKPOINT_NON_EAGER   => topDf = topDf.checkpoint(false)
      case Mode.NO_CACHE_NO_CHECKPOINT =>
    }

    val langDf = topDf.groupBy("lang").count().orderBy("lang").collectAsList()
    val yearDf = topDf.groupBy("year").count().orderBy(col("year").desc).collectAsList()
    val t1 = System.currentTimeMillis()

    println("Processing took " + (t1 - t0) + " ms.")

    println("Five-star publications per language");
    import scala.jdk.CollectionConverters._
    for (r <- langDf.asScala) {
      println(r.getString(0) + " ... " + r.getLong(1))
    }

    println("\nFive-star publications per year");
    for (r <- yearDf.asScala) {
      println(s"${r.getInt(0)} ... ${r.getLong(1)}")
    }

    t1 - t0
  }
}
