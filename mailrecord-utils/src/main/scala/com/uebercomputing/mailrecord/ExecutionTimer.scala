package com.uebercomputing.mailrecord

import org.joda.time.DateTime
import org.apache.log4j.Logger
import org.joda.time.Duration
import com.uebercomputing.time.DateUtils

trait ExecutionTimer {

  var startTime: DateTime = _
  var stopTime: DateTime = _

  def startTimer() {
    startTime = new DateTime()
  }

  def stopTimer() {
    stopTime = new DateTime()
  }

  def logTotalTime(prefixMsg: String, logger: Logger) {
    val timeDiff = DateUtils.getTimeDifferenceToSeconds(startTime, stopTime)
    val msg = prefixMsg + timeDiff
    logger.info(msg)
  }
}
