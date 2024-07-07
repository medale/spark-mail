package com.uebercomputing.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * http://spark-packages.org/package/databricks/spark-csv
 */
object DataFrameOps {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    // assumes enron.parquet sym link points to valid file

    // or read.format("parquet").load("enron.parquet").option...
    // [uuid: string, from: string, to: array<string>, cc: array<string>,
    // bcc: array<string>, dateUtcEpoch: bigint, subject: string,
    // mailFields: map<string,string>, body: string,
    // attachments: array<struct<fileName:string,size:int,mimeType:string,data:binary>>]
    val emailsDf = spark.read.parquet("enron.parquet")
    // [emailPrefix: string, Name: string, Position: string, Location: string]
    val rolesDf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("roles.csv")

    import spark.implicits._

    val stripDomainFunc = (emailAdx: String) => {
      val prefixAndDomain = emailAdx.split("@")
      prefixAndDomain(0)
    }
    val stripDomainUdf = udf(stripDomainFunc)

    val emailsWithFromPrefixDf1 = emailsDf.withColumn("fromEmailPrefix", call_udf("stripDomainFunc", col("from")))

    // if implicits._ => $ instead of emailsDf("...")
    //  SQLContext.implicits.StringToColumn(val sc: StringContext) { def $(
    val emailsWithFromPrefixDf = emailsDf.withColumn("fromEmailPrefix", stripDomainUdf($"from"))

    val emailsWithRolesDf =
      emailsWithFromPrefixDf.join(rolesDf, emailsWithFromPrefixDf("fromEmailPrefix") === rolesDf("emailPrefix"))

    // [Position: string, Location: string, count: bigint]
    val rolesCountDf = emailsWithRolesDf.groupBy("Position", "Location").count().orderBy($"count".desc)
    /*
     * take(100) res1: Array[org.apache.spark.sql.Row] = Array([Employee,Unknown,53955], [N/A,Unknown,32640],
     * [Unknown,Unknown,31858], [Manager,Risk Management Head,15619], [Vice President,Unknown,14909],
     * [Employee,Government Relation Executive,11411], [Trader,Unknown,8014], [Manager,Unknown,7489],
     * [Vice President,Vice President & Chief of Staff,7242], [Employee,Chief Operating Officer,4343],
     * [Vice President,Enron WholeSale Services,3624], [Employee,Associate,3427],
     * [CEO,Enron North America and Enron Enery Services,3138], [Manager,Logistics Manager,3041],
     * [President,Enron Global Mkts,3039], [Vice President,Government Affairs,3001],
     * [CEO,Enron America,2585], [Director,Unknown,2545], [Vice President,Regulatory Affairs,2155],
     * [Managing Director,Legal Department,2099], [President,Enron Online,1728]...
     */

    // What was Bradley McKay's position and location?
    val bradInfoDf = emailsWithRolesDf.select("from", "Position", "Location").where($"from".startsWith("brad.mckay"))
  }

}
