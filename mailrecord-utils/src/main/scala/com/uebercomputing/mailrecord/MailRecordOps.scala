package com.uebercomputing.mailrecord

import scala.collection.JavaConverters._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

case class MailRecordOps(mailRecord: MailRecord) {

  def getAttachmentsOpt(): Option[List[Attachment]] = {
    val attachments = mailRecord.getAttachments
    getAsScalaListOption(attachments)
  }

  def getToOpt(): Option[List[String]] = {
    val to = mailRecord.getTo
    getAsScalaListOption(to)
  }

  def getCcOpt(): Option[List[String]] = {
    val cc = mailRecord.getCc
    getAsScalaListOption(cc)
  }

  def getBccOpt(): Option[List[String]] = {
    val bcc = mailRecord.getBcc
    getAsScalaListOption(bcc)
  }

  def getMailFieldsAsScala(): Map[String, String] = {
    val mailFields = mailRecord.getMailFields
    val mutableMap = mailFields.asScala
    mutableMap.toMap
  }

  def getMailFieldOpt(key: String): Option[String] = {
    val mailFields = mailRecord.getMailFields
    Option(mailFields.get(key))
  }

  def getDateUtc(): DateTime = {
    val dateUtcEpoch = mailRecord.getDateUtcEpoch
    new DateTime(dateUtcEpoch, DateTimeZone.UTC)
  }

  /**
   * Returns Some[scala.collection.immutable.List[T]] if javaList is not null,
   * None otherwise (thanks, Jeff!)
   */
  private def getAsScalaListOption[T](javaList: java.util.List[T]): Option[scala.collection.immutable.List[T]] = {
    Option(javaList).map {
      java => java.asScala.toList
    }
  }

}
