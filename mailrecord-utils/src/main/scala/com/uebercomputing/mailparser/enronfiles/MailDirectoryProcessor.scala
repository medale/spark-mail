package com.uebercomputing.mailparser.enronfiles

import java.nio.file.Files
import java.nio.file.Path
import scala.io.Source
import org.apache.log4j.Logger
import com.uebercomputing.io.PathUtils
import com.uebercomputing.io.Utf8Codec
import resource.managed
import com.uebercomputing.mailrecord.MailRecord

abstract class MailDirectoryProcessor(mailDirectory: Path, userNamesToProcess: List[String] = Nil) extends MessageProcessor {

  private val logger = Logger.getLogger(classOf[MailDirectoryProcessor])

  def processMailDirectory(filter: MailRecord => Boolean): Int = {
    var mailMessagesProcessedCount = 0
    val userDirectories = PathUtils.listChildPaths(mailDirectory)
    for (userDirectory <- userDirectories) {
      if (isReadableDirectory(userDirectory)) {
        mailMessagesProcessedCount = processUserDirectory(userDirectory, mailMessagesProcessedCount, filter)
      } else {
        val errMsg = s"Mail directory layout (${userDirectory} violates assumption: " +
          "Mail directory should contain only user directories."
        throw new ParseException(errMsg)
      }
    }
    mailMessagesProcessedCount
  }

  def processUserDirectory(userDirectory: Path, processedCountSoFar: Int, filter: MailRecord => Boolean): Int =
    {
      var processedCount = processedCountSoFar
      if (userNamesToProcess == Nil || isUserDirectoryToBeProcessed(userDirectory)) {
        val userName = userDirectory.getFileName().toString()
        val folders = PathUtils.listChildPaths(userDirectory)
        for (folder <- folders) {
          if (isReadableDirectory(folder)) {
            val parentFolderName = None
            processedCount = processFolder(userName, parentFolderName,
              folder, processedCount, filter)
          }
        }
      }
      processedCount
    }

  def isUserDirectoryToBeProcessed(userDirectory: Path): Boolean = {
    val userName = userDirectory.getFileName().toString()
    userNamesToProcess.contains(userName)
  }

  def processFolder(userName: String, parentFolderName: Option[String],
                    folder: Path, processCountSoFar: Int,
                    filter: MailRecord => Boolean): Int = {
    val folderName = getFolderName(parentFolderName, folder)
    var processedCount = processCountSoFar
    val mailsOrSubdirs = PathUtils.listChildPaths(folder)
    for (mailOrSubdir <- mailsOrSubdirs) {
      if (Files.isRegularFile(mailOrSubdir)) {
        processedCount = processFile(userName, folderName, mailOrSubdir, processedCount, filter)
      } else {
        processedCount = processFolder(userName, Some(folderName), mailOrSubdir, processedCount, filter)
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

  def processFile(userName: String, folderName: String, mailFile: Path, processedCount: Int,
                  filter: MailRecord => Boolean): Int = {
    val fileName = mailFile.getFileName().toString()
    for (mailIn <- managed(Files.newInputStream(mailFile))) {
      try {
        val fileSystemMetadata = FileSystemMetadata(userName, folderName,
          fileName)
        val utf8Codec = Utf8Codec.codec
        process(fileSystemMetadata, Source.fromInputStream(mailIn)(utf8Codec), filter)
      } catch {
        case e: Exception =>
          val msg = s"Unable to process ${mailDirectory.toAbsolutePath().toString()}/${userName}/${folderName}/${fileName} due to $e"
          logger.warn(msg)
          throw new ParseException(msg)
      }
    }
    processedCount + 1
  }

  def getFolderName(parentFolderNameOpt: Option[String], folder: Path): String = {
    parentFolderNameOpt match {
      case Some(parentFolder) => s"${parentFolder}/${folder.getFileName().toString()}"
      case None               => folder.getFileName().toString()
    }
  }

  def isReadableDirectory(potentialDir: Path): Boolean = {
    return Files.isDirectory(potentialDir) && Files.isReadable(potentialDir)
  }
}
