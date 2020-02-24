package spark.sql.encoding.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
 * Class that implement different file related utilities
 */
object FileUtils {

  /**
   * Changes extension of all data files in given location
   *
   * @param hadoopConfiguration Hadoop configuration
   * @param filesLocation The location of files
   * @param newExtension New extension
   *
   * @return Int - Number of data files which extention was changed
   */
  def changeFilesExtentionInPath(hadoopConfiguration: Configuration, filesLocation: String, newExtension: String): Int = {

    //get handle to FileSystem
    val fs = FileSystem.get(hadoopConfiguration)

    //List data files
    val files = fs.globStatus(new Path(filesLocation + File.separator + "part*"))

    //change extension of each data file
    for(i <- 0 until files.size){
      val fileName = files(i).getPath.getName
      val fileNameNoExt = fileName.substring(0, fileName.lastIndexOf("."))
      fs.rename(new Path(filesLocation + File.separator + fileName), new Path(filesLocation + File.separator + fileNameNoExt.substring(0, 10) + "." + newExtension))
    }

    files.size
  }
}
