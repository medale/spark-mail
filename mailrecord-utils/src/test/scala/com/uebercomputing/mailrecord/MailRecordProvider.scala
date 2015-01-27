package com.uebercomputing.mailrecord

import java.util.HashMap

trait MailRecordProvider {

  def getMailRecord(): MailRecord = {
    val mailFields = new HashMap[String, String]()
    mailFields.put("DescriptorNodeId", "1234")
    val mailRecord = new MailRecord()
    mailRecord.setMailFields(mailFields)
    mailRecord
  }
}
