package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest

class DatePartitionerTest extends UnitTest {

  test("by day partition") {
    val partitionType = DatePartitionType.PartitionByDay
    val utcOffsetInMillis: Long = 0
    val expectedParts = List("1970", "01", "01")
    val parts = DatePartitioner.getDatePartion(partitionType, utcOffsetInMillis)
    assert(expectedParts === parts)
  }

  test("by month partition") {
    val partitionType = DatePartitionType.PartitionByMonth
    val utcOffsetInMillis = 0
    val expectedParts = List("1970", "01")
    val parts = DatePartitioner.getDatePartion(partitionType, utcOffsetInMillis)
    assert(expectedParts === parts)
  }

  test("by year partition") {
    val partitionType = DatePartitionType.PartitionByYear
    val utcOffsetInMillis = 0
    val expectedParts = List("1970")
    val parts = DatePartitioner.getDatePartion(partitionType, utcOffsetInMillis)
    assert(expectedParts === parts)
  }

}
