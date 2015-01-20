package com.uebercomputing.pst

import com.pff.PSTFile
import com.pff.PSTFolder
import scala.collection.JavaConverters._
import com.pff.PSTMessage
import org.apache.log4j.Logger
import com.uebercomputing.mailrecord.MailRecordWriter
import java.util.UUID

object PstFileToAvroProcessor {

  case class PstMetadata(mailRecordByDateWriter: MailRecordByDateWriter, folder: PSTFolder, pstFilePath: String, folderHierarchy: String) {}

  case class Metrics(var totalMessageCount: Int = 0,
                     var processableMessageCount: Int = 0,
                     var successfullyProcessedCount: Int = 0,
                     var folderCount: Int = 0) {}

  val LOGGER = Logger.getLogger(PstFileToAvroProcessor.getClass)

  def processPstFile(mailRecordByDateWriter: MailRecordByDateWriter, pstFile: PSTFile, pstFilePath: String): Metrics = {
    val rootFolder = pstFile.getRootFolder
    val folderName = rootFolder.getDisplayName
    val pstMetadata = PstMetadata(mailRecordByDateWriter, rootFolder, pstFilePath, folderName)
    val metrics = Metrics()
    processPstFolder(pstMetadata, metrics)
    metrics
  }

  def processPstFolder(pstMetadata: PstMetadata, metrics: Metrics): Unit = {
    val contentCount = pstMetadata.folder.getContentCount()
    metrics.totalMessageCount += contentCount
    if (contentCount > 0) {
      processFolderContent(pstMetadata, metrics)
    }

    if (pstMetadata.folder.hasSubfolders()) {
      val childFolders = pstMetadata.folder.getSubFolders()
      for (childFolder <- childFolders.asScala) {
        metrics.folderCount += 1
        val childFolderName = childFolder.getDisplayName
        val childHierarchy = getChildHierarchy(pstMetadata.folderHierarchy, childFolderName)
        val newPstMetadata = pstMetadata.copy(folder = childFolder, folderHierarchy = childHierarchy)
        processPstFolder(newPstMetadata, metrics)
      }
    }
  }

  def getChildHierarchy(folderHierarchy: String, childFolderName: String): String = {
    folderHierarchy + PstConstants.ParentFolderSeparator + childFolderName
  }

  def processFolderContent(pstMetadata: PstMetadata, metrics: Metrics): Unit = {
    try {
      var pstObj = pstMetadata.folder.getNextChild()
      while (pstObj != null) {
        pstObj match {
          case email: PSTMessage => {
            val uuid = UUID.randomUUID().toString()
            processEmail(uuid, email, pstMetadata, metrics)
          }
          case default => LOGGER.info(s"Found something of type ${default.getClass} - ignore")
        }
        pstObj = pstMetadata.folder.getNextChild()
      }
    } catch {
      case e: Throwable => LOGGER.error(s"Unable to process folder ${pstMetadata.folder.getDisplayName()} due to $e")
    }
  }

  def processEmail(uuid: String, email: PSTMessage, pstMetadata: PstMetadata, metrics: Metrics): Unit = {
    metrics.processableMessageCount += 1
    try {
      val mailMap = PstEmailToMapProcessor.process(uuid, email, pstMetadata.pstFilePath, pstMetadata.folderHierarchy)
      val mailRecord = PstEmailMapToMailRecordConverter.toMailRecord(mailMap)
      pstMetadata.mailRecordByDateWriter.writeMailRecord(mailRecord)
      metrics.successfullyProcessedCount += 1
    } catch {
      case e: Exception => LOGGER.error(s"Unable to process ${email.getDescriptorNodeId} in ${pstMetadata.pstFilePath} due to ${e}")
    }
  }
}
