package x.utils

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.ArrayList
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.FloatType
import java.lang.reflect.Field
import java.sql.Timestamp

/** The bean utils helps with the creation of the Schema from a bean and with
  * filling a row with the data.
  *
  * @author
  *   rambabu.posa
  */
object SparkBeanUtils {
  private var columnIndex = -1

  /** Builds a schema from the bean. The resulting schema is not directly usable
    * by Spark as it is a super set of what is needed.
    *
    * @param beanClass
    *   The bean to analyze
    * @return
    *   The Schema
    */
  def getSchemaFromBean(beanClass: Class[_]): Schema = {
    // for (v <- beanClass.getDeclaredFields) {
    //  println(s"---- ${v.getName}")
    // }
    // for (v <- beanClass.getDeclaredFields) {
    //  println(s"-- ${v.getName()}")
    //  val a = v.getAnnotation(classOf[SparkColumn])
    //  println(
    //    s"type: ${v.getType().getTypeName()}, ${v.getType().getSimpleName()}, ${v.getName()}",
    //  )
    //  if (a != null) {
    //    println(
    //      s"anno: ${a.name()}, ${a.nullable()}, ${a.`type`()}",
    //    )
    //  }
    // }
    // val fields: Map[String, StructField] =
    //  beanClass.getDeclaredFields
    //    .map(newField(_))
    //    .map(v => (v.name, v))
    //    .foldLeft(Map[String, StructField]()) { (m, s) => m + s }
    val fields = beanClass.getDeclaredFields
      .filter(_.getName() != "serialVersionUID")
      .map(newField(_))

    new Schema(fields)
  }

  /** Builds a row using the schema and the bean.
    */
  def getRowFromBean(schema: Schema, bean: Any): Row = {
    val cells = new ArrayList[AnyRef]

    // println("-----------------")
    // for ((k, v) <- schema.kv) {
    //  println(s"- $k, $v")
    // }
    // sys.exit(123)

    val clazz = bean.getClass

    // clazz.getDeclaredMethods().foreach(v => println(s"- ${v.getName}"))

    for ((k, v) <- schema.kv) {
      // TODO: NoSuchFieldException
      println(s"-- $v")
      // val field = clazz.getDeclaredField(v).get(clazz)
      val vv = clazz.getMethod(v.name).invoke(bean)
      println(s"--3 $vv")

      v.dataType match {
        case DataTypes.TimestampType => cells.add(vv.asInstanceOf[Timestamp])
        case _                       => cells.add(vv)
      }

      cells.add(vv)
    }

    RowFactory.create(cells.toArray)
  }

  /** Build the column name from the column name or the method name. This method
    * should be improved to ensure name unicity.
    */
  private def buildColumnName(
      columnName: String,
      methodName: String,
  ): String = {
    if (columnName.length > 0) return columnName
    if (methodName.length < 4) { // Very simplistic
      columnIndex += 1
      return "_c" + columnIndex
    }
    val colName = methodName.substring(3)
    if (colName.length == 0) {
      columnIndex += 1
      return "_c" + columnIndex
    }
    colName
  }

  /** Returns a Spark datatype from the method, by analyzing the method's return
    * type.
    *
    * @param method
    * @return
    */
  private def getDataTypeFromReturnType(method: Method): DataType = {
    val typeName = method.getReturnType.getSimpleName.toLowerCase
    typeName match {
      case "int" | "integer" => DataTypes.IntegerType
      case "long"            => DataTypes.LongType
      case "float"           => DataTypes.FloatType
      case "boolean"         => DataTypes.BooleanType
      case "double"          => DataTypes.DoubleType
      case "string"          => DataTypes.StringType
      case "date"            => DataTypes.DateType
      case "timestamp"       => DataTypes.TimestampType
      case "short"           => DataTypes.ShortType
      case "object"          => DataTypes.BinaryType
      case _                 => DataTypes.BinaryType
    }
  }

  /** Return true if the method passed as an argument is a getter, respecting
    * the following definition: <ul> <li>starts with get</li> <li>does not have
    * any parameter</li> <li>does not return null <li> </ul>
    *
    * @param method
    *   method to check
    * @return
    */
  private def isGetter(method: Method): Boolean = {
    if (!method.getName.startsWith("get")) return false
    if (method.getParameterTypes.length != 0) return false
    if (classOf[Unit] == method.getReturnType) return false
    true
  }

  // @return newName and field
  private def newField(v: Field): (String, StructField) = {
    val rawDataType = parseDataType(v.getType().getSimpleName())
      .getOrElse(DataTypes.NullType)

    val a = v.getAnnotation(classOf[SparkColumn])
    if (a == null) {
      return (v.getName, new StructField(v.getName, rawDataType, true))
    }

    val newName = if (a.name.isEmpty) { v.getName }
    else { a.name }

    val dataType = if (a.`type`.isEmpty) { rawDataType }
    else { parseDataType(a.`type`).getOrElse(DataTypes.NullType) }

    return (newName, new StructField(v.getName, dataType, a.nullable))
  }

  private def parseDataType(s: String): Option[DataType] = {
    s match {
      case "float"     => Some(DataTypes.FloatType)
      case "int"       => Some(DataTypes.IntegerType)
      case "long"      => Some(DataTypes.LongType)
      case "String"    => Some(DataTypes.StringType)
      case "Timestamp" => Some(DataTypes.TimestampType)
      case _           => None
    }
  }
}
