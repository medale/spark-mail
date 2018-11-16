package com.uebercomputing.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

/**
 * Utilities to work with Hadoop.
 */
object HadoopUtils {

  private val localHadoopConfig = getLocalHadoopConfiguration()

  /**
   * Converts a path string to a qualified Hadoop path
   * based on the optional hadoopConfig object. Default
   * configuration is local file system (file:///).
   */
  def getAsHadoopPath(pathStr: String, hadoopConfig: Configuration = localHadoopConfig): Path = {
    val fileSystem = FileSystem.get(hadoopConfig)
    val rawPath = new Path(pathStr)
    fileSystem.makeQualified(rawPath)
  }

  def getHadoopConfiguration(configLocation: String): Configuration = {
    val conf = new Configuration()
    conf.addResource(configLocation)
    conf
  }

  def getLocalHadoopConfiguration(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }

}
