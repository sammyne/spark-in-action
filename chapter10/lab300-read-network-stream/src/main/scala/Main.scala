import org.apache.spark.sql.SparkSession
import java.nio.file.Files
import java.io.File
import org.slf4j.LoggerFactory
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQueryException

object Main extends App {
  private val logger = LoggerFactory.getLogger(this.getClass())

  logger.debug("-> start")

  val s = SparkSession.builder
    .appName("Read lines from a network stream")
    .master("local[*]")
    .getOrCreate()

  val df =
    s.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load

  val query = df.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .start()

  try {
    query.awaitTermination()
  } catch {
    case e: StreamingQueryException =>
      logger.error("Exception while waiting for query to end {}.", e.getMessage)
  }

  logger.debug("<- done")
}
