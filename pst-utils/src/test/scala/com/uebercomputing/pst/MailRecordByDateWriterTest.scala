package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest
import org.apache.hadoop.conf.Configuration

class MailRecordByDateWriterTest extends UnitTest {

  test("f") {
    val hadoopConf = getLocalHadoopConf()
    val datePartitionType = DatePartitionType.PartitionByDay
    val byDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, "/tmp", "src/test/resources/psts/enron1.pst")
    //TODO - finish test
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }
}
