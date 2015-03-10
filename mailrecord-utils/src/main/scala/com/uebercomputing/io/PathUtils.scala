package com.uebercomputing.io

import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.Path
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.log4j.Logger

object PathUtils {

  private val logger = Logger.getLogger(PathUtils.getClass)

  /**
   * Get a list of all files/paths in parentPath directory. If the path is invalid
   * an warning gets logged and an empty List is returned. Otherwise, list of files
   * is returned. Uses nio2 with hopes that it can deal with file ending with .
   */
  def listChildPaths(parentPath: Path): List[Path] = {
    val dirStreamTry: Try[DirectoryStream[Path]] = Try(Files.newDirectoryStream(parentPath))

    val childPaths = scala.collection.mutable.ListBuffer[Path]()
    dirStreamTry match {
      case Success(dirStream) =>
        val iter = dirStream.iterator()
        while (iter.hasNext()) {
          childPaths += iter.next()
        }
        dirStream.close()
      case Failure(ex) => logger.warn(s"Unable to list files for ${parentPath} due to ${ex}")
    }
    childPaths.toList
  }
}
