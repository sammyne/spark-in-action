import java.io.File

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import utils.streaming.lib.{
  StreamingUtils,
  RecordStructure,
  FieldType,
  RecordGeneratorUtils,
  RecordWriterUtils,
}

object RecordsInFilesGeneratorApp extends App {
  private val log = LoggerFactory.getLogger(this.getClass)

  /** Streaming duration in seconds.
    */
  val streamDuration = 60;

  /** Maximum number of records send at the same time.
    */
  val batchSize = 10;

  /** Wait time between two batches of records, in seconds, with an element of
    * variability. If you say 10 seconds, the system will wait between 5s and
    * 15s, if you say 20s, the system will wait between 10s and 30s, and so on.
    */
  val waitTime = 5;

  val outputDirectory =
    if (
      this.args.length == 2
      && this.args(0).compareToIgnoreCase("--output-directory") == 0
    ) {
      new File(args(1)).mkdir()
      args(1)
    } else {
      StreamingUtils.getInputDirectory
    }

  val rs = new RecordStructure("contact")
    .add("fname", FieldType.FIRST_NAME)
    .add("mname", FieldType.FIRST_NAME)
    .add("lname", FieldType.LAST_NAME)
    .add("age", FieldType.AGE)
    .add("ssn", FieldType.SSN);

  log.debug("-> start (..., {})", outputDirectory);
  val start = System.currentTimeMillis();
  while (start + streamDuration * 1000 > System.currentTimeMillis()) {
    val maxRecord = RecordGeneratorUtils.getRandomInt(batchSize) + 1;
    RecordWriterUtils.write(
      rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
      rs.getRecords(maxRecord, false),
      outputDirectory,
    );
    try {
      Thread.sleep(
        RecordGeneratorUtils.getRandomInt(waitTime * 1000)
          + waitTime * 1000 / 2,
      );
    } catch {
      case e: InterruptedException => {} // Simply ignore the interruption }
    }
  }
}
