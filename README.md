spark-mail
==========

# Overview
The Spark Mail project contains code for a tutorial on how Apache Spark could be used to analyze email data. As data
we use the [Enron Email dataset from Carnegie Mellon University](https://www.cs.cmu.edu/~./enron/). We show how to
ETL (Extract Transform Load) the original file-per-email dataset into Apache Avro and Apache Parquet formats and then 
explore the email set using Spark.

# Building the project
The Spark Mail project uses an sbt build. See https://www.scala-sbt.org/ for how to download and install sbt.

Then from mail-spark directory:
```bash
# build "fat" jar with classes and all dependencies under 
# mailrecords-utils/target/scala-2.11/mailrecord-utils-{version}-fat.jar
sbt assembly
```

# ETL (Extract Transform Load)
The original dataset does not lend itself to scalable processing. The file set has over 500,000 small files. This would 
create over 500,000 input splits/initial partitions. Furthermore, we don't want our analytic code to have to deal with 
the parsing.

Therefore we parse the input once and aggregate the emails into the following 
[MailRecord format in Apache Avro IDL](https://github.com/medale/spark-mailrecord/blob/master/src/main/avro/com/uebercomputing/mailrecord/MailRecord.avdl):

```
@version("1.0.0")
@namespace("com.uebercomputing.mailrecord")
protocol MailRecordProtocol {

  record Attachment {
    string mimeType;
    bytes data;
  }

  record MailRecord {
    string uuid;
    string from;
    union{null, array<string>} to = null;
    union{null, array<string>} cc = null;
    union{null, array<string>} bcc = null;
    long dateUtcEpoch;
    string subject;
    union{null, map<string>} mailFields = null;
    string body;
    union{null, array<Attachment>} attachments = null;
  }
}
```

For convenience the Java MailRecord and Attachment classes generated from the MailRecord Apache Avro IDL file were 
copied under `mailrecord-utils/src/main/java/com/uebercomputing/mailrecord/MailRecord.java and Attachment.java`.

If you wanted to update the original AVDL file and regenerate new Java files, use the following to build.
This requires [Apache Maven](https://maven.apache.org/). To build this dependency and publish it to your local Maven 
repository (default ~/.m2/repository) do the following:

```
git clone https://github.com/medale/spark-mailrecord.git
cd spark-mailrecord
mvn clean install

# Update current Java definitions in spark-mail
cp -R spark-mailrecord/src/main/java/com spark-mail/mailrecord-utils/src/main/java
```

# Enron Email Dataset

## Note to Windows Users
The Enron email dataset used (see below) contains files that end with a dot
(e.g. ~/maildir/lay-k/inbox/1.).

The unit tests used actual emails from this dataset. This caused problems for
using Git from Eclipse. Checking the source code out from command line git
works.

However, on Windows these Unit tests fail because the files ending with . were
not processed correctly.

### Workaround:
Renamed the test files with a .txt extension. That fixes the unit tests.
However, to process the actual files in the Enron dataset (see below) we need
to rename each file with a .txt extension. Note: Don't use dots as the end of
a file name!!!

## Obtaining/preparing the Enron dataset

https://www.cs.cmu.edu/~./enron/ describes the Enron email dataset and provides
a download link at https://www.cs.cmu.edu/~./enron/enron_mail_20150507.tar.gz. 

This email set is a gzipped tar file of emails stored in directories. Once
downloaded, extract via:
    > tar xfz enron_mail_20150507.tar.gz   (or use tar xf as new tar autodectects compression)

This generates the following directory structure:
* maildir
     * $userName subdirectories for each user
     * $folderName subdirectories per user
          * mail messages in folder or additional subfolders

This directory structure contains over 500,000 small mail files without
attachments. These files all have the following layout:

    Message-ID: <31335512.1075861110528.JavaMail.evans@thyme>
    Date: Wed, 2 Jan 2002 09:26:29 -0800 (PST)
    From: sender@test.com
    To: rec1@test.com,rec2@test.com
    Cc:
    Bcc:
    Subject: Kelly Webb
    ...
    <Blank Line>
    Message Body

Some headers like To, Cc and Bcc or Subject can also be multiline values.

### Parsing Enron Email set into Apache Parquet binary format

This data set at 423MB compressed is small but using the default small files
format to process this via FileInputFormat creates over 500,000 splits to be
processed. By doing some pre-processing and storing all the file artifacts in
Apache Avro records we can make the analytics processing more effective.

We parse out specific headers like Message-ID (uuid), From (from) etc. and store
all the other headers in a mailFields map. We also store the body in its own
field.

#### Avro Parser
The [mailrecord-utils mailparser enronfiles Main class](https://github.com/medale/spark-mail/blob/master/mailrecord-utils/src/main/scala/com/uebercomputing/mailparser/enronfiles/AvroMain.scala)
allows us to convert the directory/file-based Enron data set into one Avro file
with all the corresponding MailRecord Avro records. To run this class from the
spark-mail root directory

```
sbt
> mailrecordUtils/console
val mailDir = "/datasets/enron/raw/maildir"
val avroOutput = "/datasets/enron/mail.avro"
val args = Array("--mailDir", mailDir,
                 "--avroOutput", avroOutput)
com.uebercomputing.mailparser.enronfiles.AvroMain.main(args)
```

#### Parquet Parser
To generate an Apache Parquet file from the emails run the following:

```
sbt
> mailrecordUtils/console
val mailDir = "/datasets/enron/raw/maildir"
val parquetOutput = "/datasets/enron/mail.parquet"
val args = Array("--mailDir", mailDir,
                 "--parquetOutput", parquetOutput)
com.uebercomputing.mailparser.enronfiles.ParquetMain.main(args)
```

Using Parquet format, we can easily analyze using our local [spark-shell](http://spark.apache.org).
All examples use the Parquet format. To use a DataFrame with Avro see 
https://spark-packages.org/package/databricks/spark-avro.

```
val mailDf = spark.read.parquet("/datasets/enron/mail.parquet")
mailDf.printSchema
root
 |-- uuid: string (nullable = true)
 |-- from: string (nullable = true)
 |-- to: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- cc: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- bcc: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- dateUtcEpoch: long (nullable = true)
 |-- subject: string (nullable = true)
 |-- mailFields: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- body: string (nullable = true)
 |-- attachments: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- fileName: string (nullable = true)
 |    |    |-- size: integer (nullable = true)
 |    |    |-- mimeType: string (nullable = true)
 |    |    |-- data: binary (nullable = true)

val doraMailsDf = mailDf.where($"from" === "dora.trevino@enron.com")
doraMailsDf.count
```

# Spark Analytics
* See spark-mail/analytics/dataset and spark-mail/analytics/rdd for Scala code.

# Exploring via Jupyter with Apache Toree Scala notebooks

* Copy notebooks from *spark-mail/notebooks* to */home/jovyan/work* to make them available from Docker notebook

* See Docker documentation at https://docs.docker.com/install/ for install on your OS
* We will be using the latest jupyter/all-spark-notebook which ran Spark 2.4.0 as of Nov 24, 2018
     * Adjust your local Spark standalone to match the all-spark-notebook version *or*
     * Check out https://github.com/jupyter/docker-stacks and adjust pyspark-notebook Spark/Hadoop and then load from adjusted all-spark-notebook locally.
* The bash script below assumes that:
     * `SPARK_HOME` environment variable points to the base of your Spark installation
     * `192.168.2.8` adjust to your local IP address (don't use `localhost` - non-routable from Docker container)
* Shared local directories with same dirs on host machine as in Docker image
     * `/dataset/enron` - is the local dir containing your data files (e.g. enron-small.parquet)
          * Spark driver runs on Docker machine
          * Executor runs on Docker host machine
     * `/home/jovyan/work` - directory containing notebook(s) to load (Jupyter notebook on Docker image runs from `/home/jovyan`)

```bash
docker pull jupyter/all-spark-notebook

# For "local" standalone Spark cluster with master/executor on local machine
# Download Spark 2.4.0 for Hadoop 2.7 from https://spark.apache.org/downloads.html
# Untar, set $SPARK_HOME to spark-2.4.0-bin-hadoop2.7 dir
$SPARK_HOME/sbin/start-master.sh --host 192.168.2.8
$SPARK_HOME/sbin/start-slave.sh spark://192.168.2.8:7077

# See "Connecting to a Spark Cluster in Standalone Mode" at 
# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html#apache-spark
docker run -p 8888:8888 -v /datasets/enron:/datasets/enron \
   -v /home/jovyan/work:/home/jovyan/work \
   --net=host --pid=host -e TINI_SUBREAPER=true \
   -e SPARK_OPTS='--master=spark://192.168.2.8:7077 --executor-memory=8g' \
   jupyter/all-spark-notebook
```

