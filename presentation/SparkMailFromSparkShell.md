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

## Start Local Spark Shell with Kryo and mailrecord uber jar

```
cd spark-mail
spark-shell --master local[4] --driver-memory 4G --executor-memory 4G \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryo.registrator=com.uebercomputing.mailrecord.MailRecordRegistrator \
--conf spark.kryoserializer.buffer.mb=128 \
--conf spark.kryoserializer.buffer.max.mb=512 \
--jars mailrecord-utils/target/mailrecord-utils-0.9.0-SNAPSHOT-shaded.jar
```

## Start email exploration

```
various spark-shell startup messages...
Spark context available as sc.
scala> :paste

import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps

val args = Array("--avroMailInput", "/opt/rpm1/jebbush/avro-monthly")
val config = CommandLineOptionsParser.getConfigOpt(args).get
val mailRecordRdd = MailRecordAnalytic.getMailRecordRdd(sc, config)

Ctrl-D

val froms = mailRecordRdd.map{record => record.getFrom}
froms.take(10)
...
<Lots of Spark output>
...
res0: Array[String] = Array(Susan.Pareigis@awi.state.fl.us, ...

```

### Some "analytics"
2000 presidential election

```
val bodies = mailRecordRdd.map{record => record.getBody}
val election = bodies.filter{body => body.contains("recount") && body.contains("dimpled chad")}
election.count
val out = election.collect()
