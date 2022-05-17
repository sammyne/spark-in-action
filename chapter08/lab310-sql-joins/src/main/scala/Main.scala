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

  // Builds the SQL query doing the join operation
  val sqlQuery = "select actor.first_name, actor.last_name, film.title, " +
    "film.description " +
    "from actor, film_actor, film " +
    "where actor.actor_id = film_actor.actor_id " +
    "and film_actor.film_id = film.film_id"

  val mySQLURL = s"jdbc:mysql://$address/sakila"
  val df = spark.read
    .jdbc(mySQLURL, "(" + sqlQuery + ") actor_film_alias", props)

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println(s"The dataframe contains ${df.count} record(s).")
}
