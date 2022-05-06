import org.apache.spark.sql.{SparkSession, Row, Dataset}
import org.apache.spark.sql.functions
import org.apache.spark.sql.Encoders
import java.util.{Arrays, List}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Main extends App {
  val s = SparkSession.builder
    .appName("CSV to dataframe to Dataset<Book> and back")
    .master("local")
    .getOrCreate()

  val df = s.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("testdata/books.csv")

  println("*** Books ingested in a dataframe")
  df.show(5)
  df.printSchema()

  import s.implicits._
  val bookDs: Dataset[Book] = df.map(rowToBook)

  println("*** Books are now in a dataset of books")
  bookDs.show(5, 17)
  bookDs.printSchema()

  var df2 = bookDs.toDF()
  df2 = df2.withColumn(
    "releaseDateAsString",
    functions
      .date_format(functions.col("releaseDate"), "M/d/yy")
      .as("MM/dd/yyyy"),
  )

  println("*** Books are back in a dataframe")
  df2.show(5, 13)
  df2.printSchema()

  /** This is a mapper class that will convert a Row to an instance of Book. You
    * have full control over it - isn't it great that sometimes you have
    * control?
    *
    * @author
    *   rambabu.posa
    */
  def rowToBook(row: Row): Book = {
    val dateAsString = row.getAs[String]("releaseDate")

    val releaseDate = LocalDate.parse(
      dateAsString,
      DateTimeFormatter.ofPattern("M/d/yy"),
    )

    Book(
      row.getAs[Int]("authorId"),
      row.getAs[String]("title"),
      releaseDate,
      row.getAs[String]("link"),
      row.getAs[Int]("id"),
    )
  }
}

case class Book(
    authorId: Int,
    title: String,
    releaseDate: LocalDate,
    link: String,
    id: Int = 0,
)
