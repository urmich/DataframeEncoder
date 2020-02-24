package spark.sql.encoding

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import spark.SparkSessionTestWrapper


class EncodingRunnerSpec extends AnyFunSuite with SparkSessionTestWrapper{

  test("run DataFrame encoder"){

    val expectedSchema = StructType(List(
      StructField("boolean", BooleanType, true),
      StructField("double", DoubleType, true),
      StructField("byte", ByteType, true),
      StructField("float", FloatType, true),
      StructField("int", IntegerType, true),
      StructField("long", LongType, true),
      StructField("short", ShortType, true),
      StructField("array_of_bool", ArrayType(BooleanType, true), true),
      StructField("array_of_double", ArrayType(DoubleType, true), true),
      StructField("array_of_byte", ArrayType(ByteType, true), true),
      StructField("array_of_float", ArrayType(FloatType, true), true),
      StructField("array_of_int", ArrayType(IntegerType, true), true),
      StructField("array_of_long", ArrayType(LongType, true), true),
      StructField("array_of_short", ArrayType(ShortType, true), true)
    ))

    val expectedDataFull = List(Row(true, 1.0d, 'A'.toByte, 2.5f, 10, 23L, 'B'.toShort,
      Array(true, false), Array(1.0d, 2.0d), Array('A'.toByte, '1'.toByte), Array(3.0f, 4.5f), Array(2020, 2098,1,1,1,1,1,1,1,1,1), Array(10L, 20L), Array('B'.toShort, 2))
        )




    val expectedDFFull = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedDataFull),
      expectedSchema)

    val df: EncodableDataFrame = DataFrameEncoding.toEncodableDataFrame(expectedDFFull)
    df.encodeForML("./output/full")
  }
}
