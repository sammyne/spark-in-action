import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

class PointAttributionUdaf extends Aggregator[Int, Int, Int] {
  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def finish(reduction: Int): Int = {
    reduction
  }

  override def merge(b1: Int, b2: Int): Int = {
    b1 + b2
  }

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt

  override def reduce(b: Int, a: Int): Int = {
    b + a
  }

  override def zero: Int = 0
}
