package spark.sql.encoding.function

import org.apache.spark.sql.Row
import spark.sql.encoding.model.{EncodedRecord, EncodedRecordHeader}

/**
 * Class implementing Dataframe partition mapping function that encodes the Rows of the dataframe into Interator of STRING
 */
class DataFramePartitionEncodingFunction extends (Iterator[Row] => Iterator[String]) with Serializable {

  /**
   * Implementation of DataFrame to Encoded value mapping function
   *
   * @param rows
   * @return String Iterator to Bits representing encoded record
   */
  override def apply(rows: Iterator[Row]): Iterator[String] = {

    //Initializing offset
    var prevRowOffset: Long = EncodedRecordHeader.HEADER_LENGTH_BYTES

    val encodedRecord: EncodedRecord = new EncodedRecord

    while (rows.hasNext) {
      //encode row by row
      prevRowOffset = encodedRecord.encodeDataFrameRow(rows.next(), prevRowOffset)
    }

    Array(encodedRecord.getEncodedValue).toIterator
  }
}
