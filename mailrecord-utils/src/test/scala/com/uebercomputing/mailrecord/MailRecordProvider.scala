package com.uebercomputing.mailrecord

import java.util.HashMap
import scala.collection.JavaConverters._

trait MailRecordProvider {

  def getMailRecord(uuid: String): MailRecord = {
    val mailRecord = getMailRecord()
    mailRecord.setUuid(uuid)
    mailRecord.setSubject(s"Subject: ${uuid}")
    mailRecord.setTo(List(s"${uuid}@to.com").asJava)
    mailRecord.setFrom(s"${uuid}@from.com")
    mailRecord.setDateUtcEpoch(42)
    mailRecord.setBody(uuid)
    mailRecord
  }

  def getMailRecord(): MailRecord = {
    val mailFields = new HashMap[String, String]()
    mailFields.put("DescriptorNodeId", "1234")
    val mailRecord = new MailRecord()
    mailRecord.setMailFields(mailFields)
    mailRecord
  }
}
