package spark.sql.encoding.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import spark.SparkSessionTestWrapper

class DataTypeUtilsSpec extends AnyFunSuite with SparkSessionTestWrapper {

  def fixture =
    new {
      val lspark = spark
    }

  test("test converting Double to binary") {
    val f = fixture
    val doubleValue: Double = 1.0d
    val expectedSchema = StructType(List(
      StructField("double", DoubleType, true)))

    val expectedData = List(Row(doubleValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertDoubleToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("0011111111110000000000000000000000000000000000000000000000000000"))(true)
  }

  test("test converting Double array to binary") {
    val f = fixture
    val doubleValue: Double = 1.0d
    val expectedSchema = StructType(List(
      StructField("double_array", ArrayType(DoubleType, true), true)))

    val expectedData = List(Row(Array(doubleValue, doubleValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertDoubleToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("0011111111110000000000000000000000000000000000000000000000000000" * 2))(true)
  }

  test("test converting Float to binary") {
    val f = fixture

    val floatValue: Float = 2.0f
    val expectedSchema = StructType(List(
      StructField("float", FloatType, true)))

    val expectedData = List(Row(floatValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertFloatToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("01000000000000000000000000000000"))(true)
  }

  test("test converting Float array to binary") {
    val f = fixture

    val floatValue: Float = 2.0f
    val expectedSchema = StructType(List(
      StructField("float_arr", ArrayType(FloatType, true), true)))

    val expectedData = List(Row(Array(floatValue, floatValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)


    assertResult(DataTypeUtils.convertFloatToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("01000000000000000000000000000000" * 2))(true)
  }

  test("test converting Int to binary") {

    val f = fixture

    val intValue: Int = 2
    val expectedSchema = StructType(List(
      StructField("int", IntegerType, true)))

    val expectedData = List(Row(intValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertIntToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("00000000000000000000000000000010"))(true)
  }

  test("test converting Int array to binary") {

    val f = fixture

    val intValue: Int = 2
    val expectedSchema = StructType(List(
      StructField("int_arr", ArrayType(IntegerType, true), true)))

    val expectedData = List(Row(Array(intValue, intValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertIntToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("00000000000000000000000000000010" * 2))(true)

  }

  test("test converting Short to binary") {

    val f = fixture

    val shortValue: Short = 2
    val expectedSchema = StructType(List(
      StructField("short", ShortType, true)))

    val expectedData = List(Row(shortValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertShortToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("0000000000000010"))(true)
  }

  test("test converting Short array to binary") {

    val f = fixture

    val shortValue: Short = 2
    val expectedSchema = StructType(List(
      StructField("short_arr", ArrayType(ShortType, true), true)))

    val expectedData = List(Row(Array(shortValue, shortValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertShortToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("0000000000000010" * 2))(true)

  }

  test("test converting Long to binary") {

    val f = fixture

    val longValue: Long = 2L
    val expectedSchema = StructType(List(
      StructField("long", LongType, true)))

    val expectedData = List(Row(longValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertLongToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("0000000000000000000000000000000000000000000000000000000000000010"))(true)
  }

  test("test converting Long Array to binary") {

    val f = fixture

    val longValue: Long = 2L
    val expectedSchema = StructType(List(
      StructField("long_arr", ArrayType(LongType, true), true)))

    val expectedData = List(Row(Array(longValue, longValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertLongToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("0000000000000000000000000000000000000000000000000000000000000010" * 2))(true)
  }

  test("test converting Byte to binary") {

    val f = fixture

    val byteValue: Byte = -128
    val expectedSchema = StructType(List(
      StructField("byte", ByteType, true)))

    val expectedData = List(Row(byteValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertByteToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("10000000"))(true)
  }

  test("test converting Byte array to binary") {

    val f = fixture

    val byteValue: Byte = -128
    val expectedSchema = StructType(List(
      StructField("byte_arr", ArrayType(ByteType, true), true)))

    val expectedData = List(Row(Array(byteValue, byteValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertByteToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("10000000" * 2))(true)
  }

  test("test converting Booelean to binary") {

    val f = fixture

    val booleanValue: Boolean = true
    val expectedSchema = StructType(List(
      StructField("boolean", BooleanType, true)))

    val expectedData = List(Row(booleanValue))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertBooleanToBinaryString(expectedDF.collect()(0).get(0), false).binaryString.
      equals("00000001"))(true)
  }

  test("test converting Booelean array to binary") {

    val f = fixture

    val booleanValue: Boolean = true
    val expectedSchema = StructType(List(
      StructField("boolean_arr", ArrayType(BooleanType, true), true)))

    val expectedData = List(Row(Array(booleanValue, !booleanValue)))

    val expectedDF = f.lspark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

    assertResult(DataTypeUtils.convertBooleanToBinaryString(expectedDF.collect()(0).get(0), true).binaryString.
      equals("0000000100000000"))(true)
  }
}
