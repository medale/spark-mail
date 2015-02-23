# Running Spark Mail Analytics from the Spark Shell

## Important Note!!
The Spark Mail framework is compiled to run under Hadoop 2.4.0 using
Avro 1.7.6 with the hadoop2 Avro mapred package (the "new" Hadoop mapreduce
API). Unfortunately, the plain vanilla download of Spark 1.2.0 for Hadoop 2.4
experienced some dependency divergence and had avro-mapred-1.7.5 (for hadoop1)
overwrite the correct avro-mapred-1.7.6-hadoop2 classes resulting in the
Spark Mail framework shows the following error message:

```
java.lang.IncompatibleClassChangeError: Found interface org.apache.hadoop.mapreduce.TaskAttemptContext, but class was expected
at org.apache.avro.mapreduce.AvroRecordReaderBase.initialize(AvroRecordReaderBase.java:87)
at org.apache.spark.rdd.NewHadoopRDD$$anon$1.<init>(NewHadoopRDD.scala:135)
```

If you get this error message when running against your Spark Shell you will
need to compile a custom version. A pull request to fix this for 1.3.x onwards was
submitted via https://github.com/apache/spark/pull/4315.

The fix for 1.2.x is shown here:
https://github.com/medale/spark/compare/apache:v1.2.1-rc2...medale:avro-hadoop2-v1.2.1-rc2

See CreatingAvroMapred2Spark.md for instructions on how to build a Spark 1.2.1
with the fix.

## Download and Compile Spark Mail code

```
git clone https://github.com/medale/spark-mail.git
cd spark-mail
# for the impatient:
mvn -DskipTests clean install
# run the tests
mvn clean install
```

## Start Local Spark Shell with Kryo and mailrecord uber jar

```
# Assumes you are in the spark-mail root folder for the --jars jar path

spark-shell --master local[4] --driver-memory 4G --executor-memory 4G \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryo.registrator=com.uebercomputing.mailrecord.MailRecordRegistrator \
--conf spark.kryoserializer.buffer.mb=128 \
--conf spark.kryoserializer.buffer.max.mb=512 \
--jars mailrecord-utils/target/mailrecord-utils-0.9.0-SNAPSHOT-shaded.jar \
--driver-java-options "-Dlog4j.configuration=log4j.properties"
```

Or

```
spark-shell --master local[4] --driver-memory 4G --executor-memory 4G \
--jars mailrecord-utils/target/mailrecord-utils-0.9.0-SNAPSHOT-shaded.jar \
--properties-file mailrecord-utils/mailrecord.conf \
--driver-java-options "-Dlog4j.configuration=log4j.properties"
```

## Start email exploration (Local)

Assumes directory is spark-mail (contains hadoop-local.xml)

```scala
various spark-shell startup messages...
Spark context available as sc.
scala> :paste

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps

val args = Array("--avroMailInput", "/opt/rpm1/enron/filemail.avro",
"--hadoopConfPath", "hadoop-local.xml")
val config = CommandLineOptionsParser.getConfigOpt(args).get
val recordsRdd = MailRecordAnalytic.getMailRecordsRdd(sc, config)

Ctrl-D

val froms = recordsRdd.map{record => record.getFrom}
froms.take(10)
...
<Lots of Spark output>
...
res0: Array[String] = Array(alexandra.villarreal@enron.com, ...

```

## Email exploration (Hadoop cluster)

In this configuration, Spark is configured to talk to the cluster by setting
$SPARK_HOME/conf/spark-env.sh:

```bash
# set to the directory that contains yarn-site.xml,
# core-site.xml etc. for your cluster
export HADOOP_CONF_DIR=/usr/lib/hadoop/conf
```

We use SparkContext's hadoopConfiguration (sc.hadoopConfiguration) to
read all the configuration properties and therefore don't need to specify
the --hadoopConfPath.

In the example below, we assume that the user executing the job has
a file enron.avro in their user directory. The example shows user hadoop
with a home directory of /user/hadoop containing enron.avro (in hdfs).

```scala
scala> :paste

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps

val args = Array("--avroMailInput", "/user/hadoop/enron.avro")
val config = CommandLineOptionsParser.getConfigOpt(args).get
val recordsRdd = MailRecordAnalytic.getMailRecordsRdd(sc, config)

### Some "analytics"

#### How many folders per user?

```
val userNameFolderTupleRdd = recordsRdd.flatMap { mailRecord =>
  val userNameOpt = mailRecord.getMailFieldOpt(AvroMessageProcessor.UserName)
  val folderNameOpt = mailRecord.getMailFieldOpt(AvroMessageProcessor.FolderName)
  if (userNameOpt.isDefined && folderNameOpt.isDefined) {
    Some((userNameOpt.get, folderNameOpt.get))
    } else {
      None
    }
  }

userNameFolderTupleRdd.cache()

val uniqueFoldersByUserRdd = userNameFolderTupleRdd.aggregateByKey(Set[String]())(
    seqOp = (set, folder) => set + folder,
    combOp = (set1, set2) => set1 ++ set2)
val folderPerUserRddExact = uniqueFoldersByUserRdd.mapValues { set => set.size }

val folderCounts: RDD[Int] = folderPerUserRddExact.values

val stats = folderCounts.stats()

//buckets 0-25, 25-50 etc.
val buckets = Array(0.0,25,50,75,100,125,150,175,200)
folderCounts.histogram(buckets, evenBuckets=true)

folderPerUserRddExact.max()(Ordering.by(tuple => tuple._2))


val folderPerUserRddEstimate = userNameFolderTupleRdd.countApproxDistinctByKey().sortByKey()

folderPerUserRddExact.saveAsTextFile("exact")
folderPerUserRddEstimate.saveAsTextFile("estimate")
```
