package com.uebercomputing.spark.sql

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Row

import com.uebercomputing.utils.DatePartitioner
import com.uebercomputing.utils.PartitionByYear

/**
 *
 */
object ParquetPartitions {

  def main(args: Array[String]): Unit = {
    val sqlContext = org.apache.spark.sql.test.TestSQLContext
    //assumes enron.parquet sym link points to valid file

    //or read.format("parquet").load("enron.parquet").option...
    val emailsDf = sqlContext.read.parquet("enron.parquet")
    emailsDf.printSchema
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
    val emailsWithYearDf = emailsDf.withColumn("year", emailsDf("uuid"))
    val extendedEmailsRdd = emailsWithYearDf.map(row => {
      val dateUtcEpoch = row.getAs[Long]("dateUtcEpoch")
      val yearList = DatePartitioner.getDatePartion(PartitionByYear, dateUtcEpoch)
      val year = yearList(0)
      val rowSeq = row.toSeq
      val newSeq = rowSeq.take(rowSeq.size - 1) ++ Seq(year)
      Row.fromSeq(newSeq)
    })
    val schema = emailsWithYearDf.schema
    val extendedEmailsDf = sqlContext.createDataFrame(extendedEmailsRdd, schema)

    extendedEmailsDf.write.format("parquet").partitionBy("year").save("/opt/rpm1/enron/parquet/out")
    /*_common_metadata  year=0001  year=1986  year=1999  year=2002  year=2007  year=2024
_metadata         year=0002  year=1997  year=2000  year=2004  year=2012  year=2043
_SUCCESS          year=1980  year=1998  year=2001  year=2005  year=2020  year=2044
part-r-00001.gz.parquet in each
*/

  }
}
