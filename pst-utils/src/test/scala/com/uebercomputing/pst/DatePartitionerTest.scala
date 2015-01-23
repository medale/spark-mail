package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest

class DatePartitionerTest extends UnitTest {

  test("by day partition") {
    val partitionType = PartitionByDay
    val utcOffsetInMillis: Long = 0
    val expectedParts = List("1970", "01", "01")
    val parts = DatePartitioner.getDatePartion(partitionType, utcOffsetInMillis)
    assert(expectedParts === parts)
  }

  test("by month partition") {
    val partitionType = PartitionByMonth
    val utcOffsetInMillis = 0
    val expectedParts = List("1970", "01")
    val parts = DatePartitioner.getDatePartion(partitionType, utcOffsetInMillis)
    assert(expectedParts === parts)
  }

  test("by year partition") {
    val partitionType = PartitionByYear
    val utcOffsetInMillis = 0
    val expectedParts = List("1970")
    val parts = DatePartitioner.getDatePartion(partitionType, utcOffsetInMillis)
    assert(expectedParts === parts)
  }

}
