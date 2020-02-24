package spark.sql.encoding.model

/**
 * Trait to mark entity that can be encoded
 */
trait Encodable {

  /**
   * Method to get Encoded Value
   * @return Encoded value
   */
  def getEncodedValue: String
}
