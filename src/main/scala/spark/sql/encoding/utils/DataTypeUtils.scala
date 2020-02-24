package spark.sql.encoding.utils

import spark.sql.encoding.utils.model.SparkToBinaryConversionInfo

import scala.collection.mutable

/**
 * DataTypes Utilities
 */
object DataTypeUtils {

  private[this] final val BOOLEAN_TYPE_NAME = "boolean"
  private[this] final val INT_TYPE_NAME = "int"
  private[this] final val FLOAT_TYPE_NAME = "float"
  private[this] final val BYTE_TYPE_NAME = "byte"
  private[this] final val LONG_TYPE_NAME = "long"
  private[this] final val SHORT_TYPE_NAME = "short"
  private[this] final val DOUBLE_TYPE_NAME = "double"

  /**
   * Convert String to binary representation string
   *
   * @param string String to convert
   * @return Binary representation string
   */
  def convertToBinaryString(string: String): String = {
    val stringBuilder: StringBuilder = new StringBuilder
    for (i <- 0 until string.length) {
      stringBuilder.append(convertToBinaryString(string.charAt(i).toByte, 1))
    }
    stringBuilder.toString()
  }

  /**
   * Convert number to a binary representation by concatinating binary representation of each digit
   *
   * @param number Number to convert
   * @param bytes  Length in bytes
   * @return Binary representation string
   */
  def convertToBinaryString(number: Long, bytes: Int): String = {
    val length = bytes * java.lang.Byte.SIZE
    //prepend leading zeros
    ("0" * length) + number.toBinaryString takeRight length
  }

  /**
   *
   * @param byteArray byte array to convert to binary string
   * @return Binary representation string that consists of concatination of binary representation of each byte
   */
  def convertToBinaryString(byteArray: Array[Byte]): String = {

    val binaryString: mutable.StringBuilder = new mutable.StringBuilder()

    if(!(byteArray == null || byteArray.size == 0)){
      for(i <- 0 until byteArray.length){
        binaryString.append(DataTypeUtils.convertToBinaryString(byteArray(i).toLong, 1))
      }
    }
    binaryString.toString()
  }

  /**
   * Converts Double/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertDoubleToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {
    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        binaryString.append(DataTypeUtils.convertToBinaryString(java.lang.Double.doubleToRawLongBits(value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Double]), java.lang.Double.BYTES))
      }
    } else {
      binaryString.append(DataTypeUtils.convertToBinaryString(java.lang.Double.doubleToRawLongBits(value.asInstanceOf[Double]), java.lang.Double.BYTES))
    }
    SparkToBinaryConversionInfo(binaryString.toString(), DOUBLE_TYPE_NAME + java.lang.Double.SIZE)
  }

  /**
   * Converts Float/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertFloatToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {
    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        binaryString.append(DataTypeUtils.convertToBinaryString(java.lang.Float.floatToRawIntBits(value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Float]), java.lang.Float.BYTES))
      }
    } else {
      binaryString.append(DataTypeUtils.convertToBinaryString(java.lang.Float.floatToRawIntBits(value.asInstanceOf[Float]), java.lang.Float.BYTES))
    }
    SparkToBinaryConversionInfo(binaryString.toString(), FLOAT_TYPE_NAME + java.lang.Float.SIZE)
  }

  /**
   * Converts Int/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertIntToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {

    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        binaryString.append(convertToBinaryString(value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Int], Integer.BYTES))
      }
    } else {
      binaryString.append(convertToBinaryString(value.asInstanceOf[Int], Integer.BYTES))
    }

    SparkToBinaryConversionInfo(binaryString.toString(), INT_TYPE_NAME + Integer.SIZE)
  }

  /**
   * Converts Long/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertLongToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {

    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        binaryString.append(convertToBinaryString(value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Long], java.lang.Long.BYTES))
      }
    } else {
      binaryString.append(convertToBinaryString(value.asInstanceOf[Long], java.lang.Long.BYTES))
    }

    SparkToBinaryConversionInfo(binaryString.toString(), LONG_TYPE_NAME + java.lang.Long.SIZE)
  }

  /**
   * Converts Boolean/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertBooleanToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {

    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        val intVal: Int = if (value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Boolean]) 1 else 0
        binaryString.append(convertToBinaryString(intVal, java.lang.Byte.BYTES))
      }
    } else {
      val intVal: Int = if (value.asInstanceOf[Boolean]) 1 else 0
      binaryString.append(convertToBinaryString(intVal, java.lang.Byte.BYTES))
    }

    SparkToBinaryConversionInfo(binaryString.toString(), BOOLEAN_TYPE_NAME + java.lang.Byte.SIZE)
  }

  /**
   * Converts Byte/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertByteToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {

    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        val intVal: Int = value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Byte].toInt
        binaryString.append(convertToBinaryString(intVal, java.lang.Byte.BYTES))
      }
    } else {
      val intVal: Int = value.asInstanceOf[Byte].toInt
      binaryString.append(convertToBinaryString(intVal, java.lang.Byte.BYTES))
    }

    SparkToBinaryConversionInfo(binaryString.toString(), BYTE_TYPE_NAME + java.lang.Byte.SIZE)
  }

  /**
   * Converts Short/s to binary string
   * @param value Value to convert
   * @param isArray Flag stating if @value is array
   * @return SparkToBinaryConversionInfo
   */
  def convertShortToBinaryString(value: Any, isArray: Boolean): SparkToBinaryConversionInfo = {

    val binaryString: StringBuilder = new StringBuilder
    if (isArray) {
      for (i <- 0 until value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array.length) {
        binaryString.append(convertToBinaryString(value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].array(i).asInstanceOf[Short], java.lang.Short.BYTES))
      }
    } else {
      binaryString.append(convertToBinaryString(value.asInstanceOf[Short], java.lang.Short.BYTES))
    }

    SparkToBinaryConversionInfo(binaryString.toString(), SHORT_TYPE_NAME + java.lang.Short.SIZE)
  }
}

