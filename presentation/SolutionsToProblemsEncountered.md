# Maven Build Challenges

## Maven Shade Plugin
The Maven Shade Plugin allows us to package uber jars with all our code
and its dependencies.

### Security Exception for Signed Jar

Symptom: SecurityException: no manifiest section for signature file ...

Cause: Shade plugin repackages signed jars

Solution: Keep Maven Shade Plugin from including crypto artifacts
(from http://stackoverflow.com/questions/8302022/maven-shade-jar-throw-exception)

```
        <plugin>
					<groupId>org.apache.maven.plugins</groupId>
					<artifactId>maven-shade-plugin</artifactId>
				...
					<configuration>
						<shadedArtifactAttached>true</shadedArtifactAttached>
						<filters>
							<filter>
								<!-- Avoid security exception for signed jars -->
								<artifact>*:*</artifact>
								<excludes>
									<exclude>META-INF/*.SF</exclude>
									<exclude>META-INF/*.RSA</exclude>
									<exclude>META-INF/*.INF</exclude>
								</excludes>
							</filter>
						</filters>
```

### Overwritten resources.conf

Symptom: ConfigException$Missing: No configuration setting found for key 'akka.version' when trying to run uber jar with Spark artifacts via jar -cp <jar>...

Cause: configuration file reference.conf for Akka gets overwritten by another file.

Solution: Append all reference.conf files (from http://apache-spark-user-list.1001560.n3.nabble.com/Packaging-a-spark-job-using-maven-td5615.html)

```
						</filters>
						<transformers>
							<transformer
								implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
								<resource>reference.conf</resource>
							</transformer>
						</transformers>
					</configuration>
				</plugin>
```
# Spark Serialization Challenges

## Task Not Serializable

Symptom: When trying to ETL the original PST files using Spark, we did a
foreach over each pst file. In the foreach block we were calling
an object that required a Hadoop configuration. When running this
we got a Task not serializable exception with NotSerializableException thrown for the Hadoop Configuration.

```
SparkException: Task not serializable
* at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:166)
* at org.apache.spark.util.ClosureCleaner$.clean(ClosureCleaner.scala:158)
* at org.apache.spark.SparkContext.clean(SparkContext.scala:1242)
* at org.apache.spark.rdd.RDD.foreach(RDD.scala:758)
* at com.uebercomputing.pst.SparkEtl$.main(SparkEtl.scala:32)
* at com.uebercomputing.pst.SparkEtl.main(SparkEtl.scala)
* Caused by: java.io.NotSerializableException: org.apache.hadoop.conf.Configuration
* at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1183)
```

Cause: Spark was trying to serialize the task itself using Java Serialization. The Hadoop configuration object is not a Java Serializable but a Writable.

Solution: Use WritableSerializable (from https://github.com/apache/spark/pull/3457/files)

```
val serializableConf = new SerializableWritable(hadoopConf)
...
//in foreach code block:
val mailRecordByDateWriter = new MailRecordByDateWriter(serializableConf.value, datePartitionType, rootPath, pstAbsolutePath)
```

# Scala That Came in Handy

## Typing list as vargs

Symptom: Trying to use Map.apply method based on a list of tuples.
```
type mismatch; found : List[(String, String)] required: (String, String)
```

Cause: Map apply takes varargs.

Solution: Explicitly type list as varargs (from http://stackoverflow.com/questions/4176440/syntax-sugar-for-treating-seq-as-method-parameters)

```
val keyVals = if (config.master.isDefined) {
  List(MailRecordSparkConfFactory.AppNameKey -> appName, MailRecordSparkConfFactory.MasterKey -> config.master.get)
  } else {
    List(MailRecordSparkConfFactory.AppNameKey -> appName)
  }
  val props = Map[String, String](keyVals: _*)
```

## Want to use Scala goodness on java.util.List object

Symptom:

    value foreach is not a member of java.util.List[com.uebercomputing.mailrecord.Attachment]

Cause: java.util.List has no foreach method

Solution:

```
import scala.collection.JavaConverters._
...
//Note: attachValue came from Map[String, Object] hence the cast
val attachments = attachValue.asInstanceOf[java.util.List[Attachment]].asScala
for (attachment <- attachments) {
  ...
```
