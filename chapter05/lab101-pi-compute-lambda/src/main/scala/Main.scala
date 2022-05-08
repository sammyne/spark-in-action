import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions
import org.apache.spark.api.java.function.{MapFunction, ReduceFunction}

import java.util.ArrayList
import org.apache.spark.sql.Encoders

object Main extends App {
  private var counter = 0

  val slices = 10
  val numberOfThrows = 100000 * slices
  println(
    "About to throw " + numberOfThrows + " darts, ready? Stay away from the target!",
  )

  val t0 = System.currentTimeMillis
  val spark = SparkSession.builder
    .appName("Spark Pi")
    .master("local[*]")
    .getOrCreate

  val t1 = System.currentTimeMillis
  println("Session initialized in " + (t1 - t0) + " ms")

  val numList = new ArrayList[Integer](numberOfThrows)

  // For  Spark Encoder implicits
  import spark.implicits._

  for (i <- 1.to(numberOfThrows))
    numList.add(i)

  val incrementalDf = spark.createDataset(numList).toDF

  val t2 = System.currentTimeMillis
  println("Initial dataframe built in " + (t2 - t1) + " ms")

  // For  Spark Encoder implicits
  import spark.implicits._

  val dartsDs = incrementalDf.map(_ => {
    val x: Double = Math.random * 2 - 1
    val y: Double = Math.random * 2 - 1
    counter += 1
    if (counter % 100000 == 0)
      println("" + counter + " darts thrown so far")
    if (x * x + y * y <= 1) 1
    else 0
  })

  val t3 = System.currentTimeMillis
  println("Throwing darts done in " + (t3 - t2) + " ms")

  val dartsInCircle = dartsDs.reduce((x, y) => x + y)
  val t4 = System.currentTimeMillis
  println("Analyzing result in " + (t4 - t3) + " ms")

  println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows)

}
