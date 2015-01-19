package com.uebercomputing.pst

import com.uebercomputing.mailrecord.MailRecord
import java.util.UUID
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

trait MailRecordProvider extends EmailProvider {

  /**
   * Wed Sep 26 06:04:00 EDT 2001
   */
  val PstDateFormatter = DateTimeFormat.forPattern("EEE MMM dd kk:mm:ss zzz YYYY")

  /**
   * 2097604, Wed Sep 26 06:04:00 EDT 2001
   * 2097636, Fri Oct 05 13:02:00 EDT 2001
   * 2099300, Mon Oct 29 23:19:46 EST 2001
   * 2099332, Mon Oct 29 18:41:30 EST 2001, attachment
   */
  def getMailRecord(descriptorIndex: Long): MailRecord = {
    val email = getEmail(descriptorIndex)
    val uuid = UUID.randomUUID().toString
    val originalPstPath = TestFilePath
    val parentFolders = ParentFolders
    val mailMap = PstEmailToMapProcessor.process(uuid, email, originalPstPath, parentFolders)
    val mailRecord = PstEmailMapToMailRecordConverter.toMailRecord(mailMap)
    mailRecord
  }

  def getAsDateTime(dateStr: String): DateTime = {
    val dateTime = PstDateFormatter.parseDateTime(dateStr)
    dateTime
  }
}
