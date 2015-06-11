package com.uebercomputing.test

import java.io.File

import com.uebercomputing.mailparser.enronfiles.AvroMain

/**
 * Trait provides functionality to create a temporary mail file based on
 * parsing all the content of a mail directory and method to delete it
 * after a test.
 */
trait TempMailFileManagerAvro {

  /**
   * Create a temporary mail file either from default mail directory or specified
   * mailDir. Parses mailDir into temporary mail file and returns handle to that file.
   */
  def parseMailDirToAvroMailFile(mailDir: String = "src/test/resources/enron/maildir"): File = {
    val tempFile = File.createTempFile("tempMail", ".avro")
    val args = Array(AvroMain.MailDirArg, mailDir,
      AvroMain.AvroOutputArg, tempFile.getAbsolutePath,
      AvroMain.OverwriteArg, true.toString())
    AvroMain.main(args)
    tempFile
  }

  def deleteFileIfItExists(tempFile: File): Boolean = {
    if (tempFile.exists()) {
      tempFile.delete()
    } else {
      false
    }
  }
}
