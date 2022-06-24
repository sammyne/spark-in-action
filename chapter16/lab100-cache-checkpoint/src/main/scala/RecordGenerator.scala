import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.ListBuffer

object RecordGenerator {
  private val cal = Calendar.getInstance();

  private val fnames = Array[String](
    "John",
    "Kevin",
    "Lydia",
    "Nathan",
    "Jane",
    "Liz",
    "Sam",
    "Ruby",
    "Peter",
    "Rob",
    "Mahendra",
    "Noah",
    "Noemie",
    "Fred",
    "Anupam",
    "Stephanie",
    "Ken",
    "Sam",
    "Jean-Georges",
    "Holden",
    "Murthy",
    "Jonathan",
    "Jean",
    "Georges",
    "Oliver"
  );
  private val lnames = Array[String](
    "Smith",
    "Mills",
    "Perrin",
    "Foster",
    "Kumar",
    "Jones",
    "Tutt",
    "Main",
    "Haque",
    "Christie",
    "Karau",
    "Kahn",
    "Hahn",
    "Sanders"
  );
  private val articles = Array[String]("The", "My", "A", "Your", "Their");
  private val adjectives = Array[String](
    "",
    "Great",
    "Beautiful",
    "Better",
    "Worse",
    "Gorgeous",
    "Terrific",
    "Terrible",
    "Natural",
    "Wild"
  );
  private val nouns = Array[String]("Life", "Trip", "Experience", "Work", "Job", "Beach");
  private val lang = Array[String]("fr", "en", "es", "de", "it", "pt");
  private val daysInMonth = Array[Int](31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);

  def createDataframe(spark: SparkSession, recordCount: Int): DataFrame = {
    println("-> createDataframe()")
    val schema = DataTypes.createStructType(
      Array[StructField](
        DataTypes.createStructField("name", DataTypes.StringType, false),
        DataTypes.createStructField("title", DataTypes.StringType, false),
        DataTypes.createStructField("rating", DataTypes.IntegerType, false),
        DataTypes.createStructField("year", DataTypes.IntegerType, false),
        DataTypes.createStructField("lang", DataTypes.StringType, false)
      )
    );

    var df: DataFrame = null
    val inc = 500000
    var recordCreated = 0L
    while (recordCreated < recordCount) {
      val recordInc = if (recordCreated + inc > recordCount) {
        recordCount - recordCreated;
      } else {
        inc
      }

      val rows = new java.util.ArrayList[Row]()
      for (j <- 0 until recordInc.toInt) {
        val row = Row(
          s"${this.randFirstName()} ${this.randLastName()}",
          this.randTitle(),
          this.randRating(),
          this.randRecentYears(25),
          this.randLang()
        )
        rows.add(row)
      }
      df = if (df == null) {
        spark.createDataFrame(rows, schema)
      } else {
        df.union(spark.createDataFrame(rows, schema))
      }
      recordCreated = df.count()
      println(s"${recordCreated} records created")
    }

    df.show(3, false)
    println("<- createDataframe()")

    df
  }

  def randAdjective(): String = this.randPick(this.adjectives)

  def randArticle(): String = this.randPick(this.articles)

  def randFirstName(): String = this.randPick(this.fnames)

  def randInt(upper: Int): Int = (Math.random() * upper).toInt

  def randLang(): String = this.randPick(this.lang)

  def randLastName(): String = this.randPick(this.lnames)

  def randNoun(): String = this.randPick(this.nouns)

  def randPick[T](arr: Array[T]): T = arr(this.randInt(arr.length))

  def randRating(): Int = this.randInt(3) + 3

  def randRecentYears(i: Int): Int = this.cal.get(Calendar.YEAR) - this.randInt(i)

  def randTitle(): String = s"${this.randArticle()} ${this.randAdjective().trim()} ${randNoun()}"

  // def randTitle():String = this.

  /*
  public static String getLang() {
    return lang[getRandomInt(lang.length)];
  }

  public static int getRecentYears(int i) {
    return cal.get(Calendar.YEAR) - getRandomInt(i);
  }

  public static int getRating() {
    return getRandomInt(3) + 3;
  }

  public static String getRandomSSN() {
    return "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-"
        + getRandomInt(10) + getRandomInt(10)
        + "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)
        + getRandomInt(10);
  }

  public static int getRandomInt(int i) {
    return (int) (Math.random() * i);
  }

  public static String getFirstName() {
    return fnames[getRandomInt(fnames.length)];
  }

  public static String getLastName() {
    return lnames[getRandomInt(lnames.length)];
  }

  public static String getArticle() {
    return articles[getRandomInt(articles.length)];
  }

  public static String getAdjective() {
    return adjectives[getRandomInt(adjectives.length)];
  }

  public static String getNoun() {
    return nouns[getRandomInt(nouns.length)];
  }

  public static String getTitle() {
    return (getArticle() + " " + getAdjective()).trim() + " " + getNoun();
  }

  public static int getIdentifier(List<Integer> identifiers) {
    int i;
    do {
      i = getRandomInt(60000);
    } while (identifiers.contains(i));

    return i;
  }

  public static Integer
      getLinkedIdentifier(List<Integer> linkedIdentifiers) {
    if (linkedIdentifiers == null) {
      return -1;
    }
    if (linkedIdentifiers.isEmpty()) {
      return -2;
    }
    int i = getRandomInt(linkedIdentifiers.size());
    return linkedIdentifiers.get(i);
  }

  public static String getLivingPersonDateOfBirth(String format) {
    int year = cal.get(Calendar.YEAR) - getRandomInt(120) + 15;
    int month = getRandomInt(12);
    int day = getRandomInt(daysInMonth[month]) + 1;
    cal.set(year, month, day);

    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(cal.getTime());
  }
   */

}
