package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest
import org.apache.hadoop.conf.Configuration

class MailRecordByDateWriterTest extends UnitTest with MailRecordProvider {

  /**
   * 2097604, Wed Sep 26 06:04:00 EDT 2001
   * 2097636, Fri Oct 05 13:02:00 EDT 2001
   * 2099300, Mon Oct 29 23:19:46 EST 2001
   * 2099332, Mon Oct 29 18:41:30 EST 2001, attachment
   */
  test("byMonthPartition") {
    val hadoopConf = getLocalHadoopConf()
    val datePartitionType = DatePartitionType.PartitionByMonth
    val byDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, "/tmp/mailrecord", "src/test/resources/psts/enron1.pst")
    val descriptorIndices = List(2097604, 2097636, 2099300, 2099332)
    for (descriptorIndex <- descriptorIndices) {
      val mailRecord = getMailRecord(descriptorIndex)
      val result = byDateWriter.writeMailRecord(mailRecord)
      assert(result)
    }
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }
}
