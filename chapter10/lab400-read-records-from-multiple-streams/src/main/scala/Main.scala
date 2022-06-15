import org.apache.spark.sql.SparkSession
import java.nio.file.Files
import java.io.File
import org.slf4j.LoggerFactory
import utils.streaming.lib.StreamingUtils
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.StructType

object Main extends App {
  // 需要设置 /src/main/resources/log4j.properties 的 `log4j.rootLogger=DEBUG, console`
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

  val inDir = StreamingUtils.getInputDirectory
  val outDir = "/tmp/dir2"

  val df1 = s.readStream
    .format("csv")
    .schema(schema)
    .load(inDir)

  val df2 = s.readStream
    .format("csv")
    .schema(schema)
    .load(outDir)

  val queryStream1 = df1.writeStream
    .outputMode(OutputMode.Append)
    .format("console")
    .foreach(new AgeChecker(1))
    .start()

  val queryStream2 = df2.writeStream
    .outputMode(OutputMode.Append)
    .format("console")
    .foreach(new AgeChecker(2))
    .start()

  val start = System.currentTimeMillis()
  var nIter = 0

  println("2")
  while (queryStream1.isActive && queryStream2.isActive) {
    println("1")
    nIter += 1
    logger.debug("Pass #{}", nIter)
    if (start + 60000 < System.currentTimeMillis) {
      queryStream1.stop()
      queryStream2.stop()
    }
    try {
      Thread.sleep(2000)
    } catch {
      case e: InterruptedException =>
      // Simply ignored
    }
  }

  logger.debug(s"<- done with #(iteration) = ${nIter}")
}
