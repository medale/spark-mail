package com.uebercomputing.spark.sql

import com.uebercomputing.utils.DatePartitioner
import com.uebercomputing.utils.PartitionByYear
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
 */
object ParquetPartitions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    // assumes enron.parquet sym link points to valid file

    // or read.format("parquet").load("enron.parquet").option...
    val emailsDf = spark.read.parquet("enron.parquet")
    emailsDf.printSchema()
    /*
 root
 |-- uuid: string (nullable = false)
 |-- from: string (nullable = false)
 |-- to: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- cc: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- bcc: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- dateUtcEpoch: long (nullable = false)
 |-- subject: string (nullable = false)
 |-- mailFields: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = false)
 |-- body: string (nullable = false)
 |-- attachments: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- fileName: string (nullable = false)
 |    |    |-- size: integer (nullable = false)
 |    |    |-- mimeType: string (nullable = false)
 |    |    |-- data: binary (nullable = false)
     */
    val getYearUdf = udf((dateUtcEpoch: Long) => {
      val yearList = DatePartitioner.getDatePartition(PartitionByYear, dateUtcEpoch)
      yearList(0)
    })
    val emailsWithYearDf = emailsDf.withColumn("year", getYearUdf(emailsDf("dateUtcEpoch")))
    emailsWithYearDf.write.format("parquet").partitionBy("year").save("/datasets/enron/parquet/out")
    /*_common_metadata  year=0001  year=1986  year=1999  year=2002  year=2007  year=2024
_metadata         year=0002  year=1997  year=2000  year=2004  year=2012  year=2043
_SUCCESS          year=1980  year=1998  year=2001  year=2005  year=2020  year=2044
part-r-00001.gz.parquet in each
     */

  }

}
