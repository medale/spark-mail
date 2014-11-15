package com.uebercomputing.mailparser

import com.uebercomputing.test.UnitTest

import java.io.InputStream
import java.util.Map

import com.uebercomputing.mailrecord.MailRecord

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import resource.managed

import scala.io.Source

class AvroMessageProcessorTest extends UnitTest {

  val TestFileUrl = "/enron/maildir/neal-s/all_documents/99."

  test("sunny day conversion from neal-s 99.") {
    val conf = new Configuration()
    conf.set("default.fsName", "file:///")
    val avroFileUriStr = "testEmails.avro"
    val fileSys = FileSystem.get(conf)
    val avroFilePath = new Path(avroFileUriStr)
    val out: FSDataOutputStream = fileSys.create(avroFilePath)
    val processor = new AvroMessageProcessor()
    processor.open(out)

    for (testFileIn <- managed(getClass().getResourceAsStream(TestFileUrl))) {
      val msgSrc = Source.fromInputStream(testFileIn)
      val pathParts = TestFileUrl.split("/")
      val userName = pathParts(2)
      val folderName = pathParts(3)
      val fileName = pathParts(4)

      val fileSystemMetadata = FileSystemMetadata(userName, folderName,
        fileName)
      val mailRecord = processor.process(fileSystemMetadata, msgSrc)

      val mailFields = mailRecord.getMailFields()
      val actualFilename = mailFields.get(AvroMessageProcessor.FileName)
      assert(fileName === actualFilename.toString())

      val actualFolderName = mailFields.get(AvroMessageProcessor.FolderName)
      assert(folderName === actualFolderName.toString())

      val actualUserName = mailFields.get(AvroMessageProcessor.UserName)
      assert(userName === actualUserName.toString())

      assert("<19546475.1075853053633.JavaMail.evans@thyme>" === mailFields.get(MessageParser.MsgId).toString())
      assert("chris.sebesta@enron.com" === mailRecord.getFrom().toString())
      assert("RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee #1 TBS - July 99 thru April 2001"
        === mailRecord.getSubject().toString())

    }
    processor.close()

    assert(fileSys.exists(avroFilePath))
    val fileStatus = fileSys.getFileStatus(avroFilePath)
    val len = fileStatus.getLen()
    assert(len > 0)
  }
}
