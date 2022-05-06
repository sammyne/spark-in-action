import org.apache.spark.sql.{SparkSession, Row, Dataset}
import org.apache.spark.sql.functions
import org.apache.spark.sql.Encoders
import java.util.{Arrays, List}

object Main extends App {
  val s = SparkSession.builder
    .appName("Array to Dataset<String>")
    .master("local")
    .getOrCreate()

  val stringList: Array[String] =
    Array[String]("Jean", "Liz", "Pierre", "Lauric")
  val data: List[String] = Arrays.asList(stringList: _*)

  val ds = s.createDataset(data)(Encoders.STRING)
  ds.show()
  ds.printSchema()
}
