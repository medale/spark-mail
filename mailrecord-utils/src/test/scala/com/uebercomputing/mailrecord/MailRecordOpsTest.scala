package com.uebercomputing.mailrecord

import com.uebercomputing.test.UnitTest

class MailRecordOpsTest extends UnitTest with MailRecordProvider {

  test("getMailFieldsAsScala") {
    val mailRecord = getMailRecord()
    val ops = MailRecordOps(mailRecord)
    val mailFields = ops.getMailFieldsAsScala()
    assert(mailFields("DescriptorNodeId") === "1234")
  }

  test("implicit conversion from mail record to mailRecordOps") {
    import com.uebercomputing.mailrecord.Implicits._

    val mailRecord = getMailRecord()
    // getToOpt is on MailRecordOps
    val toOpt = mailRecord.getToOpt()
    assert(toOpt.isDefined)
  }
}
