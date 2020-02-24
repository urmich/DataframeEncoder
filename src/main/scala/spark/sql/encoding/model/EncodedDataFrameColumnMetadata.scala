package spark.sql.encoding.model

import scala.collection.mutable.ListBuffer

/**
 * Class that represents metadata of a single DataFrame column
 */
class EncodedDataFrameColumnMetadata {

  private[this] var _dtype: String = _
  private[this] val lengths: ListBuffer[Long] = new ListBuffer[Long]
  private[this] val offsets: ListBuffer[Long] = new ListBuffer[Long]

  def dtype = _dtype

  def dtype_=(dtype: String): Unit = {
    _dtype = dtype
  }

  /**
   * Adds length to the @lengths array
   *
   * @param length The value of length
   */
  def addLength(length: Long): Unit = {
    lengths += length
  }

  /**
   * Adds offset to the @offsets array
   *
   * @param offset The value of length
   */
  def addOffset(offset: Long): Unit = {
    offsets += offset
  }

}
