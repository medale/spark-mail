package com.uebercomputing.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * http://spark-packages.org/package/databricks/spark-csv
 *
 */
object DataFrameOps {

  def main(args: Array[String]): Unit = {
    val sqlContext = org.apache.spark.sql.test.TestSQLContext
    //assumes enron.parquet sym link points to valid file

    //or read.format("parquet").load("enron.parquet").option...
    //[uuid: string, from: string, to: array<string>, cc: array<string>,
    //bcc: array<string>, dateUtcEpoch: bigint, subject: string,
    //mailFields: map<string,string>, body: string,
    //attachments: array<struct<fileName:string,size:int,mimeType:string,data:binary>>]
    val emailsDf = sqlContext.read.parquet("enron.parquet")
    //[emailPrefix: string, Name: string, Position: string, Location: string]
    val rolesDf = sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").load("roles.csv")

    import sqlContext.implicits._

    val stripDomainUdf = udf((emailAdx: String) => {
      val prefixAndDomain = emailAdx.split("@")
      prefixAndDomain(0)
    })

    val getUserName = udf((mailFields: Map[String, String]) => mailFields("UserName"))

    //if implicits._ => $ instead of emailsDf("...")
    // SQLContext.implicits.StringToColumn(val sc: StringContext) { def $(
    val emailsWithFromPrefixDf = emailsDf.withColumn("fromEmailPrefix", stripDomainUdf($"from")).
      withColumn("user", getUserName($"mailFields"))

    val emailsWithRolesDf = emailsWithFromPrefixDf.join(rolesDf,
      emailsWithFromPrefixDf("fromEmailPrefix") === rolesDf("emailPrefix"))

    //[Position: string, Location: string, count: bigint]
    val rolesCountDf = emailsWithRolesDf.groupBy("Position", "Location").count()
  }
}
