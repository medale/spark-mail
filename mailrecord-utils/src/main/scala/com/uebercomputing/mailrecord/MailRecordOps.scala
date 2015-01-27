package com.uebercomputing.mailrecord

import scala.collection.JavaConverters._

case class MailRecordOps(mailRecord: MailRecord) {

  def getAttachmentsOpt(): Option[List[Attachment]] = {
    val attachments = mailRecord.getAttachments
    if (attachments != null) {
      val scalaList = attachments.asScala.toList
      Some(scalaList)
    } else {
      None
    }
  }

  def getToOpt(): Option[List[String]] = {
    val tos = mailRecord.getTo
    if (tos != null) {
      val scalaList = tos.asScala.toList
      Some(scalaList)
    } else {
      None
    }
  }

  def getCcOpt(): Option[List[String]] = {
    val ccs = mailRecord.getCc
    if (ccs != null) {
      val scalaList = ccs.asScala.toList
      Some(scalaList)
    } else {
      None
    }
  }

  def getBccOpt(): Option[List[String]] = {
    val bccs = mailRecord.getBcc
    if (bccs != null) {
      val scalaList = bccs.asScala.toList
      Some(scalaList)
    } else {
      None
    }
  }

  def getMailFieldsAsScala(): Map[String, String] = {
    val mailFields = mailRecord.getMailFields
    val mutableMap = mailFields.asScala
    mutableMap.toMap
  }

}
