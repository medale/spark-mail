package com.uebercomputing.time

import org.joda.time.DateTime
import org.joda.time.Duration

object DateUtils {

  def getTimeDifferenceToSeconds(startTime: DateTime, endTime: DateTime): String = {
    val duration = new Duration(startTime, endTime)
    s"""Days: ${duration.getStandardDays} Hours: ${duration.getStandardHours} Minutes: ${duration.getStandardMinutes} Seconds: ${duration.getStandardSeconds}"""
  }
}
