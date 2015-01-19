package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest

class PstFileToAvroProcessorTest extends UnitTest with EmailProvider with MailRecordWriterProvider {

  test("processPstFile") {
    val tempFile = getTemporaryFile()
    val pstFile = getPstFile()
    //val pstMetadata = PstMetadata()
  }
}
