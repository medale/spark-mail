package com.uebercomputing.mailrecord

import com.uebercomputing.mailparser.enronfiles.ParsedMessageToMailRecordConverter
import java.util.UUID

trait MailRecordProvider {

  val DefaultUuid = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").toString
  val DefaultTo = Some(List(s"${DefaultUuid}@to.com"))

  def getMailRecord(uuid: String): MailRecord = {
    val mailRecord = getMailRecord()
    mailRecord.setUuid(uuid)
    mailRecord
  }

  def getMailRecord(): MailRecord = {
    val mailFields = Map[String, String]("DescriptorNodeId" -> "1234")
    val mailRecord = MailRecordOps(
      uuid = DefaultUuid,
      subject = ParsedMessageToMailRecordConverter.DefaultSubject,
      from = ParsedMessageToMailRecordConverter.DefaultFrom,
      tosOpt = DefaultTo,
      dateUtcEpoch = ParsedMessageToMailRecordConverter.DefaultDate,
      body = DefaultUuid,
      mailFieldsOpt = Some(mailFields)
    )
    mailRecord
  }

}
