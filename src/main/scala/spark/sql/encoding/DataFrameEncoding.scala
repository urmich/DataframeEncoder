package spark.sql.encoding

import org.apache.spark.sql.DataFrame

/**
 * Class that decorates the dataframe
 */
object DataFrameEncoding {
  implicit def toEncodableDataFrame(dataFrame: DataFrame): EncodableDataFrame = new EncodableDataFrame(dataFrame: DataFrame)
}
