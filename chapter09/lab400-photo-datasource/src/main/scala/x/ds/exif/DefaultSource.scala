package x.ds.exif

import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util.{Set => JavaSet, Map => JavaMap}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

import x.utils.SparkBeanUtils
import x.extlib.PhotoMetadata
import x.extlib.RecursiveExtensionFilteredLister
import config.Constants
import x.extlib.ExifUtils
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoders

class DefaultSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    this
      .getTable(null, Array.empty[Transform], options.asCaseSensitiveMap())
      .schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: JavaMap[String, String],
  ): Table = new SimpleBatchTable

}

/*
  Defines Read Support and Initial Schema
 */

class SimpleBatchTable extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = {
    this.newScanBuilder(CaseInsensitiveStringMap.empty()).build().readSchema()
  }

  override def capabilities(): JavaSet[TableCapability] = {
    import scala.jdk.CollectionConverters._

    Set(
      TableCapability.BATCH_READ,
    ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SimpleScanBuilder(options)
}

/*
   Scan object with no mixins
 */
class SimpleScanBuilder(opts: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new SimpleScan(opts)
}

/*
    Batch Reading Support

    The schema is repeated here as it can change after column pruning etc
 */

class SimpleScan(opts: CaseInsensitiveStringMap) extends Scan with Batch {
  override def readSchema(): StructType = Encoders.product[PhotoMetadata].schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new SimplePartition())
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new SimplePartitionReaderFactory(opts.asCaseSensitiveMap())
}

// simple class to organise the partition
class SimplePartition extends InputPartition

// reader factory
class SimplePartitionReaderFactory(opts: JavaMap[String, String])
    extends PartitionReaderFactory {
  override def createReader(
      partition: InputPartition,
  ): PartitionReader[InternalRow] = new SimplePartitionReader(opts)
}

// parathion reader
//class SimplePartitionReader(opts: CaseInsensitiveStringMap)
class SimplePartitionReader(opts: JavaMap[String, String])
    extends PartitionReader[InternalRow] {

  // import scala.jdk.CollectionConverters._
  // for (v <- opts.entrySet().asScala) {
  //  println(s"k = ${v.getKey()}, v= ${v.getValue()}")
  // }

  val values = {
    val v = new RecursiveExtensionFilteredLister

    v.setLimit(opts.getOrDefault(Constants.LIMIT, "100").toInt)
    v.setRecursive(opts.getOrDefault(Constants.RECURSIVE, "false").toBoolean)
    v.setPath(opts.get(Constants.PATH))

    for (ext <- opts.get(Constants.EXTENSIONS).split(",")) {
      v.addExtension(ext)
    }

    v.getFiles.map(x => ExifUtils.processFromFilename(x.getAbsolutePath))
  }

  private val encoder = Encoders
    .product[PhotoMetadata]
    .asInstanceOf[ExpressionEncoder[PhotoMetadata]]

  private var index = 0

  override def close(): Unit = {}

  override def next = index < values.length

  override def get = {
    val idx = this.index
    this.index += 1

    // @ref: https://github.com/apache/spark/commit/e7fef70fbbea08a38316abdaa9445123bb8c39e2
    this.encoder.createSerializer()(values(idx))
  }
}
