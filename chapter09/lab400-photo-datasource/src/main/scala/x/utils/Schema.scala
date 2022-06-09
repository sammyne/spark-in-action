package x.utils

import java.io.Serializable
import java.util.HashMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes

/** Stores the Spark schema as well as extra information we cannot add to the
  * (Spark) schema.
  *
  * @author
  *   rambabu.posa
  */
@SerialVersionUID(2376325490075130182L)
case class Schema(fields: Array[(String, StructField)]) extends Serializable {
  val kv = fields
    .foldLeft(Map[String, StructField]()) { (m, s) => m + s }

  val schema = DataTypes.createStructType(fields.map(v => v._2))
}
