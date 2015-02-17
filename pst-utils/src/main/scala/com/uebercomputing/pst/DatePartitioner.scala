package com.uebercomputing.pst

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

sealed trait DatePartitionType {

  val NoDatePartitionName = "all"

  /**
   * Abstract method implemented in DatePartitionType case objects.
   */
  def getDatePartition(date: DateTime): List[String]

  def format(number: Int, length: Int): String = {
    val formatString = "%0" + length + "d"
    formatString.format(number)
  }
}

case object PartitionByDay extends DatePartitionType {
  def getDatePartition(date: DateTime): List[String] = {
    val year = date.getYear
    val month = date.getMonthOfYear
    val day = date.getDayOfMonth
    List(format(year, 4), format(month, 2), format(day, 2))
  }
}
case object PartitionByMonth extends DatePartitionType {
  def getDatePartition(date: DateTime): List[String] = {
    val year = date.getYear
    val month = date.getMonthOfYear
    List(format(year, 4), format(month, 2))
  }
}
case object PartitionByYear extends DatePartitionType {
  def getDatePartition(date: DateTime): List[String] = {
    val year = date.getYear
    List(format(year, 4))
  }
}

case object NoDatePartition extends DatePartitionType {
  def getDatePartition(date: DateTime): List[String] = List(NoDatePartitionName)
}

object DatePartitioner {

  def getDatePartion(partitionType: DatePartitionType, utcOffsetInMillis: Long): List[String] = {
    val date = new DateTime(utcOffsetInMillis, DateTimeZone.UTC)
    partitionType.getDatePartition(date)
  }
}
