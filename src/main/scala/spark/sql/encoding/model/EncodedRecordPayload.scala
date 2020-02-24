package spark.sql.encoding.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}
import spark.sql.encoding.utils.DataTypeUtils
import spark.sql.encoding.utils.model.SparkToBinaryConversionInfo

/**
 * Class representing payload of encoded output record
 */
case class EncodedRecordPayload(encodedRecordMetadata: EncodedRecordMetadata) extends Encodable {

  private[this] var rowSchema: StructType = null

  private[this] val encodedPayload: StringBuilder = new StringBuilder

  /**
   * Public method that processes DataFrame row and converts it to part of payload
   *
   * @param row Row to process
   */
  def process(row: Row, prevRowOffset: Long): Long = {

    if (rowSchema == null) {
      rowSchema = row.schema
    }

    encodedRecordMetadata.init(rowSchema)

    var currentOffset: Long = prevRowOffset

    for (i <- 0 until rowSchema.fields.size) {

      val column: StructField = rowSchema.fields(i)

      var conversionInfo: SparkToBinaryConversionInfo = null
      val columnDataType: DataType = column.dataType
      val isArray: Boolean = columnDataType.isInstanceOf[ArrayType]
      var dataType: DataType = null
      if (isArray) {
        dataType = columnDataType.asInstanceOf[ArrayType].elementType
      } else {
        dataType = column.dataType
      }

      //Not supported: NullType, StringType, composite types
      dataType match {
        case DataTypes.DoubleType => conversionInfo = DataTypeUtils.convertDoubleToBinaryString(row.get(i), isArray)
        case DataTypes.BooleanType => conversionInfo = DataTypeUtils.convertBooleanToBinaryString(row.get(i), isArray)
        case DataTypes.ByteType => conversionInfo = DataTypeUtils.convertByteToBinaryString(row.get(i), isArray)
        case DataTypes.FloatType => conversionInfo = DataTypeUtils.convertFloatToBinaryString(row.get(i), isArray)
        case DataTypes.IntegerType => conversionInfo = DataTypeUtils.convertIntToBinaryString(row.get(i), isArray)
        case DataTypes.LongType => conversionInfo = DataTypeUtils.convertLongToBinaryString(row.get(i), isArray)
        case DataTypes.ShortType => conversionInfo = DataTypeUtils.convertShortToBinaryString(row.get(i), isArray)
        case _ => {
          throw new UnsupportedOperationException("Conversion of " + dataType.typeName + " to binary is not supported")
        }
      }

      encodedPayload.append(conversionInfo.binaryString)

      //Save metadata of the column encoding
      currentOffset = encodedRecordMetadata.addFieldMetadaData(
        i,
        column.name,
        conversionInfo.dataTypeName,
        conversionInfo.binaryString.length / java.lang.Byte.SIZE,
        currentOffset)
    }

    currentOffset
  }

  /**
   * Method to get Encoded Value
   *
   * @return Encoded value
   */
  override def getEncodedValue: String = {
    encodedPayload.toString()
  }
}
