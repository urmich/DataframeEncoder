package spark.sql.encoding.utils.model

/**
 * Case calss that represents result of Spark DataType conversion to binary string
 * @param binaryString String that holds binary representaiton of Spark value
 * @param dataTypeName String representing logical name of DataType
 */
case class SparkToBinaryConversionInfo(binaryString: String, dataTypeName: String) {

}
