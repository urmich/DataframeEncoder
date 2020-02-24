package spark.sql.encoding.model

import spark.sql.encoding.utils.DataTypeUtils

object EncodedRecordHeader {
  //Constant
  val magic: String = "GS"
  val VERSION_LENGTH_BYTES = 4
  val METADATA_LENGTH_BYTES = 8
  val METADATA_OFFSET_BYTES = 8
  val HEADER_LENGTH_BYTES: Int = METADATA_LENGTH_BYTES + METADATA_OFFSET_BYTES + VERSION_LENGTH_BYTES + magic.length
}

/**
 * Class representing header of encoded output record
 */
class EncodedRecordHeader extends Encodable {

  //Assuming constant
  private[this] final val version: Int = 1

  private[this] var metadataLength: Long = 0

  private[this] var payloadLength: Long = 0

  def setMetadataLength(length: Long): Unit = {
    this.metadataLength = length
  }

  def setPayloadLength(length: Long): Unit = {
    this.payloadLength = length
  }


  private[this] def convertMagicToBinaryString: String = {
    DataTypeUtils.convertToBinaryString(EncodedRecordHeader.magic)
  }

  private[this] def convertVersionToBinaryString: String = {
    DataTypeUtils.convertToBinaryString(version, EncodedRecordHeader.VERSION_LENGTH_BYTES)
  }

  private[this] def convertMetadataLengthToBinaryString: String = {
    DataTypeUtils.convertToBinaryString(this.metadataLength, EncodedRecordHeader.METADATA_LENGTH_BYTES)
  }

  private[this] def convertMetadataOffsetToBinaryString: String = {
    val metadataOffset = EncodedRecordHeader.HEADER_LENGTH_BYTES + this.payloadLength
    DataTypeUtils.convertToBinaryString(metadataOffset, EncodedRecordHeader.METADATA_OFFSET_BYTES)
  }

  /**
   * Method to get Encoded Value
   *
   * @return Encoded value
   */
  def getEncodedValue: String = {

    convertMagicToBinaryString +
      convertVersionToBinaryString +
      convertMetadataLengthToBinaryString +
      convertMetadataOffsetToBinaryString
  }

}
