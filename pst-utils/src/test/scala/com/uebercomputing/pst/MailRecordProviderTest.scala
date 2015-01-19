package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest
import org.joda.time.DateTime
import org.joda.time.DateTimeConstants

class MailRecordProviderTest extends UnitTest with MailRecordProvider {

  test("test getAsDateTime parsing") {
    val dateStr = "Tue Oct 16 23:42:10 EDT 2001"
    val dateTime = getAsDateTime(dateStr)
    assert(dateTime.getDayOfWeek === DateTimeConstants.TUESDAY)
    assert(dateTime.getMonthOfYear === DateTimeConstants.OCTOBER)
    assert(dateTime.getDayOfMonth === 16)
    assert(dateTime.getHourOfDay === 23)
    assert(dateTime.getMinuteOfHour === 42)
    assert(dateTime.getSecondOfMinute === 10)
    assert(dateTime.getYear === 2001)
  }

}
