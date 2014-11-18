package com.uebercomputing.mailparser

import java.io.File
import java.io.FileInputStream

import org.apache.logging.log4j.LogManager

import resource.managed
import scala.io.Source

abstract class MailDirectoryProcessor(mailDirectory: File, userNamesToProcess: List[String] = Nil) extends MessageProcessor {

  private val Logger = LogManager.getLogger(classOf[MailDirectoryProcessor])

  def processMailDirectory(): Int = {
    var mailMessagesProcessedCount = 0
    val userDirectories = mailDirectory.listFiles()
    for (userDirectory <- userDirectories) {
      if (isReadableDirectory(userDirectory)) {
        mailMessagesProcessedCount += processUserDirectory(userDirectory)
      } else {
        val errMsg = s"Mail directory layout (${userDirectory} violates assumption: " +
          "Mail directory should contain only user directories."
        throw new ParseException(errMsg)
      }
    }
    mailMessagesProcessedCount
  }

  def processUserDirectory(userDirectory: File): Int =
    {
      var processedCount = 0
      if (userNamesToProcess == Nil || isUserDirectoryToBeProcessed(userDirectory)) {
        val userName = userDirectory.getName()
        val folders = userDirectory.listFiles()
        for (folder <- folders) {
          if (isReadableDirectory(folder)) {
            val parentFolderName = None
            processedCount += processFolder(userName, parentFolderName,
              folder)
          }
        }
      }
      return processedCount
    }

  def isUserDirectoryToBeProcessed(userDirectory: File): Boolean = {
    val userName = userDirectory.getName()
    userNamesToProcess.contains(userName)
  }

  def processFolder(userName: String, parentFolderName: Option[String],
                    folder: File): Int = {
    val folderName = getFolderName(parentFolderName, folder)
    var processedCount = 0
    val mailsOrSubdirs = folder.listFiles()
    for (mailOrSubdir <- mailsOrSubdirs) {
      if (mailOrSubdir.isFile()) {
        val fileName = mailOrSubdir.getName()
        for (mailIn <- managed(new FileInputStream(mailOrSubdir))) {
          try {
            val fileSystemMetadata = FileSystemMetadata(userName, folderName,
              fileName)
            process(fileSystemMetadata, Source.fromInputStream(mailIn))
            processedCount += 1
            if (processedCount % 100 == 0) {
              print(".")
            }
          } catch {
            case e: Exception =>
              // scalastyle:off
              val msg = s"Unable to process ${mailDirectory.getAbsolutePath()}/${userName}/${folderName}/${fileName} due to $e"
              Logger.warn(msg)
              // scalastyle:on
              throw new ParseException(msg)
          }
        }
      } else {
        processedCount += processFolder(userName, Some(folderName), mailOrSubdir)
      }
    }
    return processedCount
  }

  def getFolderName(parentFolderNameOpt: Option[String], folder: File): String = {
    parentFolderNameOpt match {
      case Some(parentFolder) => s"${parentFolder}/${folder.getName()}"
      case None               => folder.getName()
    }
  }

  def isReadableDirectory(potentialDir: File): Boolean = {
    return potentialDir.isDirectory() && potentialDir.canRead()
  }
}
