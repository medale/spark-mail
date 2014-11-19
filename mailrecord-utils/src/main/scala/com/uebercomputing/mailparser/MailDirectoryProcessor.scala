package com.uebercomputing.mailparser

import java.io.File
import java.io.FileInputStream

import org.apache.logging.log4j.LogManager

import resource.managed
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

abstract class MailDirectoryProcessor(mailDirectory: File, userNamesToProcess: List[String] = Nil) extends MessageProcessor {

  private val Logger = LogManager.getLogger(classOf[MailDirectoryProcessor])

  //Source.fromInputStream has implicit codec argument
  //See http://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def processMailDirectory(): Int = {
    var mailMessagesProcessedCount = 0
    val userDirectories = mailDirectory.listFiles()
    for (userDirectory <- userDirectories) {
      if (isReadableDirectory(userDirectory)) {
        mailMessagesProcessedCount = processUserDirectory(userDirectory, mailMessagesProcessedCount)
      } else {
        val errMsg = s"Mail directory layout (${userDirectory} violates assumption: " +
          "Mail directory should contain only user directories."
        throw new ParseException(errMsg)
      }
    }
    mailMessagesProcessedCount
  }

  def processUserDirectory(userDirectory: File, processedCountSoFar: Int): Int =
    {
      var processedCount = processedCountSoFar
      if (userNamesToProcess == Nil || isUserDirectoryToBeProcessed(userDirectory)) {
        val userName = userDirectory.getName()
        val folders = userDirectory.listFiles()
        for (folder <- folders) {
          if (isReadableDirectory(folder)) {
            val parentFolderName = None
            processedCount = processFolder(userName, parentFolderName,
              folder, processedCount)
          }
        }
      }
      processedCount
    }

  def isUserDirectoryToBeProcessed(userDirectory: File): Boolean = {
    val userName = userDirectory.getName()
    userNamesToProcess.contains(userName)
  }

  def processFolder(userName: String, parentFolderName: Option[String],
                    folder: File, processCountSoFar: Int): Int = {
    val folderName = getFolderName(parentFolderName, folder)
    var processedCount = processCountSoFar
    val mailsOrSubdirs = folder.listFiles()
    for (mailOrSubdir <- mailsOrSubdirs) {
      if (mailOrSubdir.isFile()) {
        processedCount = processFile(userName, folderName, mailOrSubdir, processedCount)
      } else {
        processedCount = processFolder(userName, Some(folderName), mailOrSubdir, processedCount)
      }
      if (processedCount % 10000 == 0) {
        println()
        println(s"Processed count now: $processedCount")
      } else if (processedCount % 100 == 0) {
        print(".")
      }
    }
    processedCount
  }

  def processFile(userName: String, folderName: String, mailFile: File, processedCount: Int): Int = {
    val fileName = mailFile.getName()
    for (mailIn <- managed(new FileInputStream(mailFile))) {
      try {
        val fileSystemMetadata = FileSystemMetadata(userName, folderName,
          fileName)
        //uses implicit member codec UTF-8
        process(fileSystemMetadata, Source.fromInputStream(mailIn))
      } catch {
        case e: Exception =>
          // scalastyle:off
          val msg = s"Unable to process ${mailDirectory.getAbsolutePath()}/${userName}/${folderName}/${fileName} due to $e"
          Logger.warn(msg)
          // scalastyle:on
          throw new ParseException(msg)
      }
    }
    processedCount + 1
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
