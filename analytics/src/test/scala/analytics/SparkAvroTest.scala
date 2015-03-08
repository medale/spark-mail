package analytics

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.test.TempMailFileManager
import com.uebercomputing.test.UnitTest
import org.apache.log4j.Logger
import org.apache.log4j.Level

class SparkAvroTest extends UnitTest with TempMailFileManager {

  test("Run basic Spark job against local Avro file") {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    val tempMailFile = parseMailDirToAvroMailFile()
    val sparkConf = new SparkConf().setAppName("Spark Avro Test").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val job = Job.getInstance()
    FileInputFormat.setInputPaths(job, tempMailFile.getAbsolutePath)

    val recordsKeyValues = sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[AvroKeyInputFormat[MailRecord]],
      classOf[AvroKey[MailRecord]],
      classOf[NullWritable])

    val mailRecords = recordsKeyValues.map {
      recordKeyValueTuple =>
        val mailRecord = recordKeyValueTuple._1.datum()
        (mailRecord.getFrom, mailRecord.getSubject)
    }
    val grouped = mailRecords.groupBy(record => record._1)
    println(grouped.collect().mkString(","))
    deleteFileIfItExists(tempMailFile)
    sc.stop()
  }
}
