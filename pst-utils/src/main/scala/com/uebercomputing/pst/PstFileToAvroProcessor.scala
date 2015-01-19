package com.uebercomputing.pst

import com.pff.PSTFile
import com.pff.PSTFolder
import scala.collection.JavaConverters._
import com.pff.PSTMessage
import org.apache.log4j.Logger
import com.uebercomputing.mailrecord.MailRecordWriter

object PstFileToAvroProcessor {

  case class PstMetadata(mailRecordWriter: MailRecordWriter, folder: PSTFolder, pstFilePath: String, folderHierarchy: String) {}

  case class Metrics(var totalMessageCount: Int = 0, var processableMessageCount: Int = 0, var successfullyProcessedCount: Int = 0) {}

  val LOGGER = Logger.getLogger(PstFileToAvroProcessor.getClass)

  def processPstFile(pstFile: PSTFile, pstFilePath: String): Metrics = {
    val rootFolder = pstFile.getRootFolder
    val folderName = rootFolder.getDisplayName
    Metrics()
  }

  def processPstFolder(pstMetadata: PstMetadata, metrics: Metrics): Unit = {
    var folderTotal = 0
    val contentCount = pstMetadata.folder.getContentCount()
    metrics.totalMessageCount += contentCount
    if (contentCount > 0) {
      processFolderContent(pstMetadata, metrics)
    }

    if (pstMetadata.folder.hasSubfolders()) {
      val childFolders = pstMetadata.folder.getSubFolders()
      for (childFolder <- childFolders.asScala) {
        val childFolderName = childFolder.getDisplayName
        val childHierarchy = pstMetadata.folderHierarchy + PstConstants.ParentFolderSeparator + childFolderName
        val newPstMetadata = pstMetadata.copy(folder = childFolder, folderHierarchy = childHierarchy)
        processPstFolder(newPstMetadata, metrics)
      }
    }
  }

  def processFolderContent(pstMetadata: PstMetadata, metrics: Metrics): Unit = {
    try {
      var pstObj = pstMetadata.folder.getNextChild()
      while (pstObj != null) {
        pstObj match {
          case email: PSTMessage => processEmail(email, pstMetadata, metrics)
          case default           => LOGGER.info(s"Found something of type ${default.getClass} - ignore")
        }
        pstObj = pstMetadata.folder.getNextChild()
      }
    } catch {
      case e: Throwable => LOGGER.error(s"Unable to process folder ${pstMetadata.folder.getDisplayName()} due to $e")
    }
  }

  def processEmail(email: PSTMessage, pstMetadata: PstMetadata, metrics: Metrics): Unit = {

  }
}
