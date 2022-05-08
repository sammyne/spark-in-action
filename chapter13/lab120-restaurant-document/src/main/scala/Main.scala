import org.apache.spark.sql.{Column, Dataset, Row, SparkSession, functions}

import java.util.Arrays

import org.slf4j.LoggerFactory

object Main extends App {
  private val log = LoggerFactory.getLogger(getClass)

  val TEMP_COL = "temp_column"

  // Creates a session on a local master
  val spark = SparkSession.builder
    .appName("Building a restaurant fact sheet")
    .master("local[*]")
    .getOrCreate

  // Ingests businesses into dataframe
  val businessDf = spark.read
    .format("csv")
    .option("header", true)
    .load("testdata/businesses.csv")

  // Ingests businesses into dataframe
  val inspectionDf = spark.read
    .format("csv")
    .option("header", true)
    .load("testdata/inspections.csv")

  // Shows at most 3 rows from the dataframe
  businessDf.show(3)
  businessDf.printSchema()

  inspectionDf.show(3)
  inspectionDf.printSchema()

  val factSheetDf = nestedJoin(
    businessDf,
    inspectionDf,
    "business_id",
    "business_id",
    "inner",
    "inspections",
  )

  factSheetDf.show(3)
  factSheetDf.printSchema()

  /** Builds a nested document from two dataframes.
    *
    * @param leftDf
    *   : The left or master document.
    * @param rightDf
    *   : The right or details.
    * @param leftJoinCol
    *   : Column to link on in the left dataframe.
    * @param rightJoinCol:
    *   Column to link on in the right dataframe.
    * @param joinType
    *   : Type of joins, any type supported by Spark.
    * @param nestedCol
    *   : Name of the nested column.
    * @return
    */
  def nestedJoin(
      leftDf: Dataset[Row],
      rightDf: Dataset[Row],
      leftJoinCol: String,
      rightJoinCol: String,
      joinType: String,
      nestedCol: String,
  ): Dataset[Row] = {

    // Performs the join
    var resDf = leftDf.join(
      rightDf,
      rightDf.col(rightJoinCol) === leftDf.col(leftJoinCol),
      joinType,
    )

    // Makes a list of the left columns (the columns in the master)
    // val leftColumns: Array[Column] = getColumns(leftDf)
    val leftColumns = getColumns(leftDf)

    if (log.isDebugEnabled) {
      log.debug(
        "  We have {} columns to work with: {}",
        leftColumns.length,
        leftColumns,
      )
      log.debug("Schema and data:")
      resDf.printSchema()
      resDf.show(3)
    }

    // Copies all the columns from the left/master
    val allColumns: Array[Column] =
      Arrays.copyOf(leftColumns, leftColumns.length + 1)

    // Adds a column, which is a structure containing all the columns from
    // the details
    allColumns(leftColumns.length) =
      functions.struct(getColumns(rightDf): _*).alias(TEMP_COL)

    // Performs a select on all columns
    resDf = resDf.select(allColumns: _*)

    if (log.isDebugEnabled) {
      log.debug("  Before nested join, we have {} rows.", resDf.count)
      resDf.printSchema()
      resDf.show(3)
    }

    resDf = resDf
      .groupBy(leftColumns: _*)
      .agg(functions.collect_list(functions.col(TEMP_COL)).as(nestedCol))

    if (log.isDebugEnabled) {
      resDf.printSchema()
      resDf.show(3)
      log.debug("  After nested join, we have {} rows.", resDf.count)
    }

    resDf
  }

  private def getColumns(df: Dataset[Row]): Array[Column] = {
    val fieldnames = df.columns
    val columns = new Array[Column](fieldnames.length)
    var i = 0
    for (fieldname <- fieldnames) {
      columns(i) = df.col(fieldname)
      i = i + 1
    }
    columns
  }
}
