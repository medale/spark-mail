package com.uebercomputing.mailrecord

import scala.language.implicitConversions

object Implicits {

  /**
   * Provides extra Scala-centric utility method on top of mail record object.
   */
  implicit def mailRecordToMailRecordOps(mailRecord: MailRecord): MailRecordOps = MailRecordOps(mailRecord)

}
