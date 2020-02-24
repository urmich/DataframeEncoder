package spark.sql.encoding.model

import org.apache.spark.sql.Row

/**
 * Class representing encoded output record
 */
class EncodedRecord extends Encodable {

  val encodedRecordHeader: EncodedRecordHeader = new EncodedRecordHeader
  val encodedRecordMetadata: EncodedRecordMetadata = new EncodedRecordMetadata
  val encodedRecordPayload: EncodedRecordPayload = new EncodedRecordPayload(this.encodedRecordMetadata)
  /**
   * Public method to encode each Row of DataFrame
   *
   * @param row
   */
  def encodeDataFrameRow(row: Row, prevRowOffset: Long): Long = {
    encodedRecordPayload.process(row, prevRowOffset)
  }

  /**
   * Method to get Encoded Value
   *
   * @return Encoded value
   */
  override def getEncodedValue: String = {
    encodedRecordHeader.setMetadataLength(this.encodedRecordMetadata.getEncodedValue.length)
    encodedRecordHeader.setPayloadLength(this.encodedRecordPayload.getEncodedValue.length)
    encodedRecordHeader.getEncodedValue +
      encodedRecordPayload.getEncodedValue +
      encodedRecordMetadata.getEncodedValue
  }
}
