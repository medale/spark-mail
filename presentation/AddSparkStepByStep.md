# Preparation
* Download Spark Mail: git clone git@github.com:medale/spark-mail.git
* cd spark-mail
* ln -s ~/enron/enron.avro enron.avro
* mvn clean install
* Download Spark Mail Docker: git clone
* Start Scala IDE (Spark Mail already imported)
* Start Chrome

    * [Spark API docs](http://spark.apache.org/docs/1.3.1/api/scala/index.html#org.apache.spark.rdd.RDD)
    * [Spark Mail FolderAnalyticsDriver](https://github.com/medale/spark-mail/blob/master/hadoop-example/src/main/java/com/uebercomputing/hadoop/FolderAnalyticsDriver.java)

# Spark Installation Slide
* Start Docker image

Window1:
```
cd ~/git-src/spark-mail-docker
docker run -v /home/medale:/home/medale -P -i -t -h sandbox \
medale/spark-mail-docker:v1.3.1 /etc/bootstrap.sh bash
```

# Spark Interactive Shell
```
From docker:
./start-spark.sh
```

# RDD Scaladocs
* http://spark.apache.org/docs/1.3.1/api/scala/index.html - search RDD

# Spark Analytic
```scala
import org.apache.spark.rdd._
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import com.uebercomputing.mailparser.enronfiles.MessageProcessor

val hadoopConf = sc.hadoopConfiguration

val mailRecordsAvroRdd =
sc.newAPIHadoopFile("enron.avro",
classOf[AvroKeyInputFormat[MailRecord]],
classOf[AvroKey[MailRecord]],
classOf[NullWritable], hadoopConf)

val recordsRdd = mailRecordsAvroRdd.map {
  case(avroKey, _) => avroKey.datum()
}

val tupleRdd: RDD[(String,String)] =

 recordsRdd.flatMap { mailRecord =>
  val userNameOpt =
     mailRecord.getMailFieldOpt(
       MessageProcessor.UserName)
  val folderNameOpt =
     mailRecord.getMailFieldOpt(
       MessageProcessor.FolderName)

  if (userNameOpt.isDefined &&
    folderNameOpt.isDefined) {
    Some((userNameOpt.get,
      folderNameOpt.get))
    } else {
      None
    }
 }

tupleRdd.cache()

tupleRdd.count()
tupleRdd.count()

```

# Spark Web UI
* Open second command line Window
```bash
cd ~/git-src/spark-mail-docker
./runAliasedFirefox.sh
```

# From Sets of folderNames per user
```scala
import scala.collection.mutable.{ Set => MutableSet }

val uniqueFoldersByUserRdd:
RDD[(String, MutableSet[String])] =
tupleRdd.aggregateByKey(MutableSet[String]())(
  seqOp = (folderSet, folder) => folderSet + folder,
  combOp = (set1, set2) => set1 ++ set2)

val foldersPerUserRdd: RDD[(String, Int)] =
  uniqueFoldersByUserRdd.mapValues { set => set.size }  

foldersPerUserRdd.count()

foldersPerUserRdd.first()

foldersPerUserRdd.collect()

foldersPerUserRdd.take(3)

foldersPerUserRdd.max()(Ordering.by(_._2))

foldersPerUserRdd.min()(Ordering.by(_._2))

foldersPerUserRdd.sample(false, 0.1)

val folderCounts: RDD[Int] =
   foldersPerUserRdd.values

val stats = folderCounts.stats()

val buckets = Array(0.0,25,50,75,100,125,150,175,200)
folderCounts.histogram(buckets, evenBuckets=true)

folderCounts.toDebugString
```
