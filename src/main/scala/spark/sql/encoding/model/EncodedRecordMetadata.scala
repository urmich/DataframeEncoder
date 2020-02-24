package spark.sql.encoding.model

import org.apache.spark.sql.types.StructType
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import spark.sql.encoding.utils.DataTypeUtils

import scala.collection.mutable.ListBuffer

/**
 * Class representing metadata of encoded output record
 */
class EncodedRecordMetadata extends Encodable {

  final val INIT_OFFSET_BYTES: Long = EncodedRecordHeader.HEADER_LENGTH_BYTES

  //holds the entire metadata
  private[this] val metadataMap: java.util.Map[String, EncodedDataFrameColumnMetadata] = new java.util.HashMap[String, EncodedDataFrameColumnMetadata]

  private[this] var rowSchema: StructType = null

  private[this] var numOfColumns: Int = 0

  /**
   * Public method to initialize metadata
   *
   * @param rowSchema - StructType representing Row schema
   */
  def init(rowSchema: StructType): Unit = {

    //initialization was already done
    if (this.rowSchema != null) {
      return
    }

    this.rowSchema = rowSchema
    numOfColumns = this.rowSchema.fields.size

    for (i <- 0 to numOfColumns - 1) {
      //initialize content of metadataMap
      def columnName: String = this.rowSchema.fields(i).name

      metadataMap.put(columnName, new EncodedDataFrameColumnMetadata)
    }
  }

  /**
   * Saves metadata of column encoding
   * @param columnIndex Index of the column in Row that was encoded
   * @param columnName Name of the column that was encoded
   * @param dataTypeName Data type name of the column that was encoded
   * @param length Length in Bytes of the column encoded value
   * @param offset Offset of the column encoded value
   * @return Offset of next column encoded value
   */
  def addFieldMetadaData(columnIndex: Int, columnName: String, dataTypeName: String, length: Long, offset: Long): Long = {

    val encodedDataFrameColumnMetadata = metadataMap.get(columnName)

    //Populate dtype
    encodedDataFrameColumnMetadata.dtype = dataTypeName

    //Add length to lengths array
    encodedDataFrameColumnMetadata.addLength(length)

    //Add offset to offsets array
    encodedDataFrameColumnMetadata.addOffset(offset)

    //return new offset for next field
    offset + length
  }

  /**
   * Method to get Encoded Value.
   * Encoded value is created by encoding JSON that represents the @metadataMap map
   *
   * @return Encoded value
   */
  override def getEncodedValue: String = {

    val objectMapper: ObjectMapper = new ObjectMapper()
    objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false)
    val bytes: Array[Byte] = objectMapper.writeValueAsBytes(this.metadataMap)

    DataTypeUtils.convertToBinaryString(bytes)
  }
}
