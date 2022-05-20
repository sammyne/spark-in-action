import org.apache.spark.sql.SparkSession
import java.util.Properties

object WithoutPartition extends App {
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
    .appName("MySQL to Dataframe using JDBC without partioning")
    .master("local[*]")
    .getOrCreate

  // Using properties
  val props = new Properties
  props.put("user", user)
  props.put("password", password)

  val mysqlURL = s"jdbc:mysql://$address/sakila"

  var df = spark.read.jdbc(mysqlURL, "film", props)

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println(s"The dataframe contains ${df.count} record(s).")
  println(
    s"The dataframe is split over ${df.rdd.getNumPartitions} partition(s).",
  )
}
