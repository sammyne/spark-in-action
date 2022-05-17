import org.apache.spark.sql.SparkSession
import java.util.Properties

object Main extends App {
  val (address, user, password) = this.args.toList match {
    case address :: Nil                      => (address, "root", "helloworld")
    case address :: user :: Nil              => (address, user, "helloworld")
    case address :: user :: password :: tail => (address, user, password)
    case _ =>
      throw new IllegalArgumentException(s"miss user, password, mysql-address")
  }

  /** The processing code.
    */
  val spark: SparkSession = SparkSession.builder
    .appName("MySQL to Dataframe using a JDBC Connection")
    .master("local[*]")
    .getOrCreate

  // Using properties
  val props = new Properties
  props.put("user", user)
  props.put("password", password)
  // props.put("useSSL", "false")

  val mysqlURL = s"jdbc:mysql://$address/sakila"

  var df = spark.read.jdbc(mysqlURL, "actor", props)

  df = df.orderBy(df.col("last_name"))

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println(s"The dataframe contains ${df.count} record(s).")
}
