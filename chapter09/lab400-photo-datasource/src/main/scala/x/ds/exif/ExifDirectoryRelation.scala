package x.ds.exif

import java.io.Serializable
import java.util.ArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.StructType

import x.extlib.{ExifUtils, PhotoMetadata, RecursiveExtensionFilteredLister}
import x.utils.{Schema, SparkBeanUtils}
import scala.collection.mutable.ArrayBuffer

/** Build a relation to return the EXIF data of photos in a directory.
  *
  * @author
  *   rambabu.posa
  */
class ExifDirectoryRelation
    extends BaseRelation
    with Serializable
    with TableScan {
  var sqlCtxt: SQLContext = null
  var s: Schema = null
  var photoLister: RecursiveExtensionFilteredLister = null

  override def sqlContext: SQLContext = sqlCtxt

  def setSqlContext(s: SQLContext): Unit = {
    sqlCtxt = s
  }

  /** Build and returns the schema as a StructType.
    */
  override def schema: StructType = {
    if (s == null) {
      s = SparkBeanUtils.getSchemaFromBean(classOf[PhotoMetadata])
    }

    s.schema
  }

  override def buildScan(): RDD[Row] = {
    println("-> buildScan()")
    this.schema
    // I have isolated the work to a method to keep the plumbing code
    //  as simple as possible.
    val table = collectData()
    val sparkContext = sqlCtxt.sparkContext

    import scala.jdk.CollectionConverters._
    val rowRDD = sparkContext.parallelize(table).map { photo: PhotoMetadata =>
      SparkBeanUtils.getRowFromBean(s, photo)
    }
    println("hello")
    rowRDD
  }

  /** Interface with the real world: the "plumbing" between Spark and existing
    * data, in our case the classes in charge of reading the information from
    * the photos.
    *
    * The list of photos will be "mapped" and transformed into a Row.
    */
  private def collectData(): List[PhotoMetadata] = {
    photoLister.getFiles
      .map(x => ExifUtils.processFromFilename(x.getAbsolutePath))
  }

  def setPhotoLister(p: RecursiveExtensionFilteredLister): Unit = {
    photoLister = p
  }

}
