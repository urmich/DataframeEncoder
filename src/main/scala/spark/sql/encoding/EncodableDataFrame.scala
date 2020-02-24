package spark.sql.encoding

import org.apache.spark.sql.{DataFrame, Encoders, SaveMode}
import spark.sql.encoding.function.DataFramePartitionEncodingFunction
import spark.sql.encoding.utils.FileUtils

/**
 * Class that supports encoding for DataFrame
 *
 * @param dataFrame
 */
class EncodableDataFrame(dataFrame: DataFrame) {

  val FILE_EXTENSION: String = "xyz"

  /**
   * Encode the DataFrame into required format
   *
   * @param outputPath
   */
  def encodeForML(outputPath: String): Unit = {

    //Writing the DF after encoding each partition
    dataFrame
      .mapPartitions(new DataFramePartitionEncodingFunction())(Encoders.STRING)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    //change TEXT file name extention to required one
    FileUtils.changeFilesExtentionInPath(dataFrame.sparkSession.sparkContext.hadoopConfiguration, outputPath, FILE_EXTENSION)

  }
}
