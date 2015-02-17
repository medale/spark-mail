package com.uebercomputing.pst

import java.io.File
import scala.io.Source
import com.uebercomputing.io.FileExtensionFilter

/**
 * Utility just to make sure we downloaded all pst files. Expects one argument, the name of the
 * directory where the pst files are located.
 *
 * Note: OrganizedCampaigns20040514.7z is not a valid pst file its in a different format.
 * ~07+July+2003.pst.tmp has invalid file header for PST file.
 */
object CompletenessUtils {

  def getFileNamesFromUrls(urls: Set[String]): Set[String] = {
    val fileNameRegex = "^.*/(.*?)$".r
    urls.map { url =>
      url match {
        case fileNameRegex(fileName) => {
          //println(s"Matched $fileName from $url")
          fileName
        }
        case default => {
          sys.error(s"Did not match ${default}")
        }
      }
    }
  }

  def readPstUrls(): Set[String] = {
    Source.fromFile(new File("src/main/resources/psts/pst-links.txt")).getLines().toSet
  }

  def main(args: Array[String]): Unit = {
    val mailDirStr = args(0)
    val mailDir = new File(mailDirStr)
    val pstPaths = mailDir.listFiles(new FileExtensionFilter(".pst", ".PST")).map { f => f.getName() }.toSet
    val urls = readPstUrls()
    val fileNames = getFileNamesFromUrls(urls)
    println(fileNames.diff(pstPaths).mkString("\n"))
  }
}
