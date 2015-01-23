package com.uebercomputing.time

import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.format.PeriodFormat

object DateUtils {

  def getTimeDifferenceToSeconds(startTime: DateTime, endTime: DateTime): String = {
    val duration = new Duration(startTime, endTime)
    PeriodFormat.getDefault().print(duration.toPeriod())
  }
}
