package com.uebercomputing.mailrecord

object Implicits {

  /**
   * Provides extra Scala-centric utility method on top of mail record object.
   */
  implicit def mailRecordToMailRecordOps(mailRecord: MailRecord) = MailRecordOps(mailRecord)

}
