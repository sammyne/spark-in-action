package x.ds.exif

import org.apache.spark.sql.sources.DataSourceRegister

/** Defines the "short name" for the data source
  *
  * @author
  *   rambabu.posa
  */
class ExifDirectoryDataSourceShortnameAdvertiser
    extends ExifDirectoryDataSource
    with DataSourceRegister {

  override def shortName(): String = "exif"

}
