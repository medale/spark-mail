package com.uebercomputing.mailparser

import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.logging.log4j.LogManager

import resource.managed
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

import com.uebercomputing.io.PathUtils

abstract class MailDirectoryProcessor(mailDirectory: Path, userNamesToProcess: List[String] = Nil) extends MessageProcessor {

  private val Logger = LogManager.getLogger(classOf[MailDirectoryProcessor])

  //Source.fromInputStream has implicit codec argument
  //See http://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def processMailDirectory(): Int = {
    var mailMessagesProcessedCount = 0
    val userDirectories = PathUtils.listChildPaths(mailDirectory)
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

  def processUserDirectory(userDirectory: Path, processedCountSoFar: Int): Int =
    {
      var processedCount = processedCountSoFar
      if (userNamesToProcess == Nil || isUserDirectoryToBeProcessed(userDirectory)) {
        val userName = userDirectory.getFileName().toString()
        val folders = PathUtils.listChildPaths(userDirectory)
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

  def isUserDirectoryToBeProcessed(userDirectory: Path): Boolean = {
    val userName = userDirectory.getFileName().toString()
    userNamesToProcess.contains(userName)
  }

  def processFolder(userName: String, parentFolderName: Option[String],
                    folder: Path, processCountSoFar: Int): Int = {
    val folderName = getFolderName(parentFolderName, folder)
    var processedCount = processCountSoFar
    val mailsOrSubdirs = PathUtils.listChildPaths(folder)
    for (mailOrSubdir <- mailsOrSubdirs) {
      if (Files.isRegularFile(mailOrSubdir)) {
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

  def processFile(userName: String, folderName: String, mailFile: Path, processedCount: Int): Int = {
    val fileName = mailFile.getFileName().toString()
    for (mailIn <- managed(Files.newInputStream(mailFile))) {
      try {
        val fileSystemMetadata = FileSystemMetadata(userName, folderName,
          fileName)
        //uses implicit member codec UTF-8
        process(fileSystemMetadata, Source.fromInputStream(mailIn))
      } catch {
        case e: Exception =>
          // scalastyle:off
          val msg = s"Unable to process ${mailDirectory.toAbsolutePath().toString()}/${userName}/${folderName}/${fileName} due to $e"
          Logger.warn(msg)
          // scalastyle:on
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
