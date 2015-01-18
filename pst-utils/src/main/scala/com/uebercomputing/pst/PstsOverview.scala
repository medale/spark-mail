package com.uebercomputing.pst

import java.io.File
import java.util.Date

import scala.collection.JavaConverters.asScalaBufferConverter

import com.pff.PSTFile
import com.pff.PSTFolder
import com.pff.PSTMessage

/**
 * Run:
 * java -classpath target/pst-utils-0.9.0-SNAPSHOT-shaded.jar com.uebercomputing.pst.PstOverview /opt/rpm1/jebbush > overview.txt
 *
 * Based on Java example at https://code.google.com/p/java-libpst/.
 */
object PstOverview {

  val DefaultPstHome = "/opt/rpm1/jebbush"

  def main(args: Array[String]): Unit = {
    var currPstPath = ""
    try {
      val pstHome = if (args.length > 0) args(0) else DefaultPstHome
      val baseDir = new File(pstHome)
      val pstPaths = baseDir.listFiles(new PstFileFilter())
      var grandTotal = 0
      val startTime = new Date()
      println(s"Start time: ${startTime}")
      pstPaths.foreach { pstPath =>
        currPstPath = pstPath.getName
        val pstFile = new PSTFile(pstPath)
        println(pstFile.getMessageStore().getDisplayName())
        val totalForFolder = processFolder(pstFile.getRootFolder(), 0)
        grandTotal += totalForFolder
        println(s"\n\nTotal for file $pstPath was $totalForFolder")
      }
      println(s"Grand total number of emails was $grandTotal")
      val stopTime = new Date()
      println(s"Stop time: ${stopTime}")
      val runtimeSecs = (stopTime.getTime - startTime.getTime) / 1000
      println(s"Ran for ${runtimeSecs}")
    } catch {
      case e: Throwable => sys.error(s"Unable to process $currPstPath due to $e")
    }
  }

  def processFolder(folder: PSTFolder, depth: Int): Int = {
    var folderTotal = 0
    if (depth > 0) {
      //root folder at depth 0 has no display name
      printDepth(depth)
      print(folder.getDisplayName())
      val contentCount = folder.getContentCount()
      println(s" $contentCount")
      folderTotal += contentCount
      if (contentCount > 0) {
        processFolderContent(folder, depth + 1)
      }
    }

    if (folder.hasSubfolders()) {
      val childFolders = folder.getSubFolders()
      for (childFolder <- childFolders.asScala) {
        folderTotal += processFolder(childFolder, depth + 1)
      }
    }
    folderTotal
  }

  def processFolderContent(folder: PSTFolder, depth: Int): Unit = {
    try {
      var pstObj = folder.getNextChild()
      while (pstObj != null) {
        pstObj match {
          case email: PSTMessage => //processEmail(email, depth)
          case default           => println(s"Found something of type ${default.getClass}")
        }
        pstObj = folder.getNextChild()
      }
    } catch {
      case e: Throwable => println(s"Unable to process folder ${folder.getDisplayName()} due to $e")
    }
  }

  def processEmail(email: PSTMessage, depth: Int): Unit = {
    printDepth(depth)
    println(email.getSubject())
  }

  def printDepth(depth: Int): Unit = {
    for (i <- 0 to depth) {
      print(" | ")
    }
    print(" |- ")
  }
}
