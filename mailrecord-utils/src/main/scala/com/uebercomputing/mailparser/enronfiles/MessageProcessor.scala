package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import scala.io.Source

abstract class MessageProcessor {

  def process(fileSystemData: FileSystemMetadata, mailIn: Source): MailRecord
}
