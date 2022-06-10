package x.extlib

import java.io.Serializable
import java.nio.file.attribute.FileTime
import java.sql.Timestamp
import java.util.Date
import org.slf4j.LoggerFactory
import scala.beans.BeanProperty
import scala.annotation.meta.getter
import scala.annotation.meta.beanGetter
import scala.annotation.meta.field

/** A good old JavaBean containing the EXIF properties as well as the
  * SparkColumn annotation.
  *
  * @author
  *   rambabu.posa
  */
case class PhotoMetadata(
    dateTaken: Timestamp = new Timestamp(System.currentTimeMillis()),
    directory: String,
    extension: String,
    fileCreationDate: Timestamp,
    fileLastAccessDate: Timestamp,
    fileLastModifiedDate: Timestamp,
    filename: String,
    geoX: Float = 0.0f,
    geoY: Float = 0.0f,
    geoZ: Float = 0.0f,
    height: Int = 0,
    mimeType: String,
    name: String,
    size: Long = 0L,
    width: Int = 0,
)

// Pending methods
/** def setDateTaken(date: Date): Unit = { if (date == null) {
  * PhotoMetadataScala.log.warn("Attempt to set a null date.") return }
  * setDateTaken(new Timestamp(date.getTime)) }
  *
  * def setFileCreationDate(creationTime: FileTime): Unit = {
  * setFileCreationDate(new Timestamp(creationTime.toMillis)) }
  *
  * def setFileLastAccessDate(lastAccessTime: FileTime): Unit = {
  * setFileLastAccessDate(new Timestamp(lastAccessTime.toMillis)) }
  *
  * def setFileLastModifiedDate(lastModifiedTime: FileTime): Unit = {
  * setFileLastModifiedDate(new Timestamp(lastModifiedTime.toMillis)) }
  */
