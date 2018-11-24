package com.uebercomputing.utils

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

/**
  * Given a date, return a list of the elements that we want to
  * use for partition. For example, PartitionByDay returns a List[String](yyyy,mm,dd).
  */
sealed trait DatePartitionType {

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

  /**
    * Returns list of yyyy, mm, dd based on date param.
    * @param date
    * @return List(yyyy,mm,dd)
    */
  def getDatePartition(date: DateTime): List[String] = {
    val year = date.getYear
    val month = date.getMonthOfYear
    val day = date.getDayOfMonth
    List(format(year, 4), format(month, 2), format(day, 2))
  }
}

case object PartitionByMonth extends DatePartitionType {

  /**
    * Returns list of yyyy, mm based on date param.
    * @param date
    * @return List(yyyy,mm)
    */
  def getDatePartition(date: DateTime): List[String] = {
    val year = date.getYear
    val month = date.getMonthOfYear
    List(format(year, 4), format(month, 2))
  }
}

case object PartitionByYear extends DatePartitionType {

  /**
    * Returns list of yyyy based on date param.
    * @param date
    * @return List(yyyy)
    */
  def getDatePartition(date: DateTime): List[String] = {
    val year = date.getYear
    List(format(year, 4))
  }
}

object DatePartitioner {

  /**
    * Convert utcOffsetInMillis to UTC time zone date and extract the desired date parts
    * based on the partitionType param (e.g. year or year,mm or year,mm,dd)
    * @param partitionType
    * @param utcOffsetInMillis
    * @return
    */
  def getDatePartition(partitionType: DatePartitionType, utcOffsetInMillis: Long): List[String] = {
    val date = new DateTime(utcOffsetInMillis, DateTimeZone.UTC)
    partitionType.getDatePartition(date)
  }
}
