package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import scala.io.Source

object MessageProcessor {
  val UserName = "UserName"
  val FolderName = "FolderName"
  val FileName = "FileName"
  val MailRecordFields = List("Uuid", "From", "To", "Cc", "Bcc", "Date", "Subject", "Body")
}

abstract class MessageProcessor {

  def process(fileSystemData: FileSystemMetadata, mailIn: Source, filter: MailRecord => Boolean): MailRecord
}
