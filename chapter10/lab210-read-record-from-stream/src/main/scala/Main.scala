import org.apache.spark.sql.SparkSession
import java.nio.file.Files
import java.io.File
import org.slf4j.LoggerFactory
import utils.streaming.lib.StreamingUtils
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.StructType

object Main extends App {
  private val logger = LoggerFactory.getLogger(this.getClass())

  logger.debug("-> start")

  val s = SparkSession.builder
    .appName("Read lines from a file stream")
    .master("local[*]")
    .getOrCreate()

  val schema = new StructType()
    .add("fname", "string")
    .add("mname", "string")
    .add("lname", "string")
    .add("age", "integer")
    .add("ssn", "string")

  val df = s.readStream
    .format("csv")
    .schema(schema)
    .load(StreamingUtils.getInputDirectory())

  val query = df.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", false)
    .option("numRows", 3)
    .start()

  try {
    query.awaitTermination(60000)
  } catch {
    case e: StreamingQueryException =>
      logger.error("Exception while waiting for query to end {}.", e.getMessage)
  }

  logger.debug("<- done")
}
