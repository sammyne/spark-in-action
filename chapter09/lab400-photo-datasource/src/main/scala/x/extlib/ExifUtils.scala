package x.extlib

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.util.TimeZone
import com.drew.imaging.ImageMetadataReader
import com.drew.imaging.ImageProcessingException
import com.drew.lang.Rational
import com.drew.metadata.{Metadata, MetadataException}
import com.drew.metadata.exif.ExifSubIFDDirectory
import com.drew.metadata.exif.GpsDirectory
import com.drew.metadata.jpeg.JpegDirectory
import java.sql.Timestamp
import java.nio.file.attribute.FileTime

object ExifUtils {

  def processFromFilename(absolutePathToPhoto: String): PhotoMetadata = {
    // val photo = new PhotoMetadata

    val filename = absolutePathToPhoto

    // Info from file
    val photoFile = new File(absolutePathToPhoto)
    val (name, size, directory) =
      (photoFile.getName(), photoFile.length(), photoFile.getParent());

    val file = Paths.get(absolutePathToPhoto)
    var attr: BasicFileAttributes = null
    try attr = Files.readAttributes(file, classOf[BasicFileAttributes])
    catch {
      case e: IOException =>
        println(
          f"I/O error while reading attributes of $absolutePathToPhoto. Got ${e.getMessage}. Ignoring attributes.",
        )
    }

    val (fileCreationDate, fileLastAccessDate, fileLastModifiedDate) = {
      var fileCreationDate = new Timestamp(System.currentTimeMillis())
      var fileLastAccessDate = new Timestamp(System.currentTimeMillis())
      var fileLastModifiedDate = new Timestamp(System.currentTimeMillis())
      if (attr != null) {
        // photo.setFileCreationDate(attr.creationTime)
        fileCreationDate = new Timestamp(attr.creationTime.toMillis())
        // photo.setFileLastAccessDate(attr.lastAccessTime)
        fileLastAccessDate = new Timestamp(attr.lastAccessTime.toMillis())
        // photo.setFileLastModifiedDate(attr.lastModifiedTime)
        fileLastModifiedDate = new Timestamp(attr.lastModifiedTime().toMillis())
      }

      (fileCreationDate, fileLastAccessDate, fileLastModifiedDate)
    }

    // Extra info
    var mimeType = "image/jpeg"

    val extension =
      absolutePathToPhoto.substring(absolutePathToPhoto.lastIndexOf('.') + 1)
    // photo.setExtension(extension)
    // Info from EXIF
    var metadata: Metadata = null
    try metadata = ImageMetadataReader.readMetadata(photoFile)
    catch {
      case e: ImageProcessingException =>
        println(
          s"Image processing exception while reading $absolutePathToPhoto. Got ${e.getMessage}. Cannot extract EXIF Metadata.",
        )
      case e: IOException =>
        println(
          s"I/O error while reading $absolutePathToPhoto. Got ${e.getMessage}. Cannot extract EXIF Metadata.",
        )
    }
    if (metadata == null) {
      return PhotoMetadata(
        directory = directory,
        extension = extension,
        fileCreationDate = fileCreationDate,
        fileLastAccessDate = fileLastAccessDate,
        fileLastModifiedDate = fileLastModifiedDate,
        filename = filename,
        mimeType = mimeType,
        name = name,
        size = size,
      )
    }

    val jpegDirectory = metadata.getFirstDirectoryOfType(classOf[JpegDirectory])
    val (height, width) = {
      var (h, w) = (0, 0)
      try {
        h = jpegDirectory.getInt(1)
        w = jpegDirectory.getInt(3)
      } catch {
        case e: MetadataException =>
          println(
            s"Issue while extracting dimensions from $absolutePathToPhoto. Got ${e.getMessage}. Ignoring dimensions.",
          )
      }
      (h, w)
    }
    val exifSubIFDDirectory =
      metadata.getFirstDirectoryOfType(classOf[ExifSubIFDDirectory])
    val dateTaken = {
      var v = new Timestamp(System.currentTimeMillis())
      if (exifSubIFDDirectory != null) {
        val d = exifSubIFDDirectory.getDate(36867, TimeZone.getTimeZone("EST"))
        if (d != null) { v = new Timestamp(d.getTime) }
      }
      v
    }

    val gpsDirectory = metadata.getFirstDirectoryOfType(classOf[GpsDirectory])
    val (x, y, z) = {
      var (x, y, z) = (0.0f, 0.0f, 0.0f);
      if (gpsDirectory != null) {
        x = getDecimalCoordinatesAsFloat(
          gpsDirectory.getString(1),
          gpsDirectory.getRationalArray(2),
        )

        try
          y = getDecimalCoordinatesAsFloat(
            gpsDirectory.getString(3),
            gpsDirectory.getRationalArray(4),
          )
        catch {
          case e: Exception =>
            println(
              s"Issue while extracting longitude GPS info from $absolutePathToPhoto. Got ${e.getMessage} (${e.getClass.getName}). Ignoring GPS info.",
            )
        }
        try {
          val r = gpsDirectory.getRational(6)
          if (r != null) {
            z = 1f * r.getNumerator / r.getDenominator
          }
        } catch {
          case e: Exception =>
            println(
              s"Issue while extracting altitude GPS info from $absolutePathToPhoto. Got ${e.getMessage} (${e.getClass.getName}). Ignoring GPS info.",
            )
        }
      }

      (x, y, z)
    }

    PhotoMetadata(
      dateTaken,
      directory,
      extension,
      fileCreationDate,
      fileLastAccessDate,
      fileLastModifiedDate,
      filename,
      x,
      y,
      z,
      height,
      mimeType,
      name,
      size,
      width,
    )
  }

  private def getDecimalCoordinatesAsFloat(
      orientation: String,
      coordinates: Array[Rational],
  ): Float = {
    if (orientation == null) {
      println("GPS orientation is null, should be N, S, E, or W.")
      return Float.MaxValue
    }
    if (coordinates == null) {
      println("GPS coordinates are null.")
      return Float.MaxValue
    }
    if (
      coordinates(0).getDenominator == 0 || coordinates(
        1,
      ).getDenominator == 0 || coordinates(2).getDenominator == 0
    ) {
      println("Invalid GPS coordinates (denominator should not be 0).")
      return Float.MaxValue
    }
    var m = 1
    if (
      orientation.toUpperCase.charAt(0) == 'S' || orientation.toUpperCase
        .charAt(0) == 'W'
    ) { m = -1 }
    val deg = coordinates(0).getNumerator / coordinates(0).getDenominator
    val min = coordinates(1).getNumerator * 60 * coordinates(2).getDenominator
    val sec = coordinates(2).getNumerator
    val den =
      3600 * coordinates(1).getDenominator * coordinates(2).getDenominator
    m * (deg + (min + sec) / den)
  }

}
