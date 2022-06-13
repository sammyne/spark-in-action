import org.apache.spark.sql.SparkSession
import java.nio.file.Files
import java.io.File
import org.slf4j.LoggerFactory
import utils.streaming.lib.StreamingUtils
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQueryException

object Main extends App {
  private val logger = LoggerFactory.getLogger(this.getClass())

  logger.debug("-> start")

  val s = SparkSession.builder
    .appName("Read lines from a file stream")
    .master("local[*]")
    .getOrCreate()

  val df = s.readStream.format("text").load(StreamingUtils.getInputDirectory())

  val query = df.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", false)
    .option("numRows", 3)
    .start()

  try {
    query.awaitTermination()
  } catch {
    case e: StreamingQueryException =>
      logger.error("Exception while waiting for query to end {}.", e.getMessage)
  }

  logger.debug("<- done")
}
