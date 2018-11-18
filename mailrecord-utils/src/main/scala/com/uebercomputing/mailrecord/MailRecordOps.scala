package com.uebercomputing.mailrecord

import scala.collection.JavaConverters._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

case class MailRecordOps(mailRecord: MailRecord) {

  def getAttachmentsOpt(): Option[Seq[Attachment]] = {
    val attachments = mailRecord.attachments
    attachments
  }

  def getToOpt(): Option[Seq[String]] = {
    val to = mailRecord.to
    to
  }

  def getCcOpt(): Option[Seq[String]] = {
    val cc = mailRecord.cc
    cc
  }

  def getBccOpt(): Option[Seq[String]] = {
    val bcc = mailRecord.bcc
    bcc
  }

  def getMailFieldsAsScala(): Map[String, String] = {
    val mailFields = mailRecord.mailFields
    mailFields.get
  }

  def getMailFieldOpt(key: String): Option[String] = {
    val mailFields = mailRecord.mailFields.get
    mailFields.get(key)
  }

  def getDateUtc(): DateTime = {
    val dateUtcEpoch = mailRecord.dateUtcEpoch
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
