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
    .appName("MySQL with where clause to Dataframe using a JDBC Connection")
    .master("local[*]")
    .getOrCreate

  // Using properties
  val props = new Properties
  props.put("user", user)
  props.put("password", password)

  val sqlQuery = "select * from film where " +
    "(title like \"%ALIEN%\" or title like \"%victory%\" " +
    "or title like \"%agent%\" or description like \"%action%\") " +
    "and rental_rate>1 " +
    "and (rating=\"G\" or rating=\"PG\")"

  val mySQLURL = s"jdbc:mysql://$address/sakila"
  // Let's look for all movies matching the query
  val df = spark.read
    .jdbc(mySQLURL, "(" + sqlQuery + ") film_alias", props)

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println(s"The dataframe contains ${df.count} record(s).")
}
