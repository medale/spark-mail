package com.uebercomputing.mailrecord

import com.uebercomputing.test.UnitTest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.File
import java.util.UUID

class MailRecordParquetWriterTest extends UnitTest with MailRecordProvider {

  test("write/read test") {
    val hadoopConf = getLocalHadoopConf()
    val fileSystem = FileSystem.get(hadoopConf)
    val localTempDirName = System.getProperty("java.io.tmpdir")
    val uuids = (1 to 3).map(i => UUID.randomUUID().toString())
    println(uuids.mkString(","))
    val localParquetFile = new File(localTempDirName, s"${uuids(0)}.parq")
    val path = fileSystem.makeQualified(new Path(localParquetFile.getAbsolutePath))
    try {
      println(path)
      val writer = new MailRecordParquetWriter()
      writer.open(path)
      uuids.foreach(uuid => {
        val record = getMailRecord(uuid)
        writer.append(record)
      })
      writer.close()

      val reader = new MailRecordParquetReader()
      reader.open(path)
      uuids.foreach(uuid => {
        val hasNext = reader.readNext()
        assert(hasNext)
        val record = reader.next()
        assert(record.getUuid() === uuid)
      })
      reader.close()
    } finally {
      localParquetFile.delete()
    }
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }

}
