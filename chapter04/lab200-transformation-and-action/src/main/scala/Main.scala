import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Main extends App {
  val mode = if (this.args.length == 0) {
    "noop"
  } else {
    this.args(0)
  }

  val t0 = System.currentTimeMillis

  val s = SparkSession.builder
    .appName("Analysing Catalyst's behavior")
    .master("local")
    .getOrCreate()

  val t1 = System.currentTimeMillis
  println("1. Creating a session ........... " + (t1 - t0))

  // Step 2 - Reads a CSV file with header, stores it in a dataframe
  var df = s.read
    .format("csv")
    .option("header", "true")
    .load(
      "testdata/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv",
    )

  val initalDf = df
  val t2 = System.currentTimeMillis
  println("2. Loading initial dataset ...... " + (t2 - t1))

  // Step 3 - Build a bigger dataset
  for (_ <- 0.until(60)) {
    df = df.union(initalDf)
  }
  val t3 = System.currentTimeMillis
  println("3. Building full dataset ........ " + (t3 - t2))

  // Step 4 - Cleanup. preparation
  df = df
    .withColumnRenamed("Lower Confidence Limit", "lcl")
    .withColumnRenamed("Upper Confidence Limit", "ucl")

  val t4 = System.currentTimeMillis
  println("4. Clean-up ..................... " + (t4 - t3))

  // Step 5 - Transformation
  if (mode.compareToIgnoreCase("noop") != 0) {
    df = df
      .withColumn("avg", functions.expr("(lcl+ucl)/2"))
      .withColumn("lcl2", functions.col("lcl"))
      .withColumn("ucl2", functions.col("ucl"))
    if (mode.compareToIgnoreCase("full") == 0)
      df = df.drop("avg", "lcl2", "ucl2")
  }

  val t5 = System.currentTimeMillis
  println("5. Transformations  ............. " + (t5 - t4))

  // Step 6 - Action
  df.collect
  val t6 = System.currentTimeMillis
  println("6. Final action ................. " + (t6 - t5))

  println("")
  println("# of records .................... " + df.count)
}
