spark-mail
==========

# Overview
The Spark Mail project contains code for a tutorial on how Apache Spark could be used to analyze email data. As data
we use the [Enron Email dataset from Carnegie Mellon University](https://www.cs.cmu.edu/~./enron/). We show how to
ETL (Extract Transform Load) the original file-per-email dataset into Apache Avro and Apache Parquet formats and then 
explore the email set using Spark.

# Building the project
The Spark Mail project uses an sbt build. See https://www.scala-sbt.org/ for how to download and install sbt. Or
install via sdkman (https://sdkman.io/):

```bash
curl -s "https://get.sdkman.io" | bash
sdk install sbt
```

Then from mail-spark directory:
```bash
# build "fat" jar with classes and all dependencies under 
# mailrecords-utils/target/scala-2.13/mailrecord-utils-{version}-fat.jar
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

Create an `enron` directory and define the `ENRON_HOME` environment variable to point
to that directory. In the example below we define `ENRON_DIR` as `$HOME/datasets/enron`.
Under `ENRON_DIR` we create a `raw` directory ($ENRON_DIR/raw), where we copy the tar gz.

This email set is a gzipped tar file of emails stored in directories. Once
downloaded and moved to `$ENRON_HOME/raw` we extract it which creates a `maildir` subdirectory
as `$ENRON_HOME/raw/maildir`:

```bash
mkidr -p $HOME/datasets/enron
export ENRON_HOME=$HOME/datasets/enron
mkdir $ENRON_HOME/raw 
cd $ENRON_HOME/raw
mv ~/Downloads/enron_mail_20150507.tar.gz .
tar xfz enron_mail_20150507.tar.gz
```

This generates the following directory structure:
* $ENRON_HOME/raw/maildir
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
val homeDir = sys.props("user.home")
val mailDir = s"$homeDir/datasets/enron/raw/maildir"
val avroOutput = s"$homeDir/datasets/enron/mail.avro"
val args = Array("--mailDir", mailDir,
                 "--avroOutput", avroOutput)
com.uebercomputing.mailparser.enronfiles.AvroMain.main(args)
```

#### Parquet Parser
To generate an Apache Parquet file from the emails run the following:

```
sbt
> mailrecordUtils/console
val homeDir = sys.props("user.home")
val mailDir = s"$homeDir/datasets/enron/raw/maildir"
val parquetOutput = s"$homeDir/datasets/enron/mail.parquet"
val args = Array("--mailDir", mailDir,
                 "--parquetOutput", parquetOutput)
com.uebercomputing.mailparser.enronfiles.ParquetMain.main(args)
```

Using Parquet format, we can easily analyze using our local [spark-shell](http://spark.apache.org).
All examples use the Parquet format. To use a DataFrame with Avro see 
https://spark-packages.org/package/databricks/spark-avro.

```
val homeDir = sys.props("user.home")
val mailDf = spark.read.parquet(s"$homeDir/datasets/enron/mail.parquet")
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
* Install spark via sdkman: `curl -s "https://get.sdkman.io" | bash` 
* `sdk install spark 3.5.1`

```bash
export SPARK_HOME=$HOME/.sdkman/candidates/spark/current
cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
cp $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties
```

* Edit $SPARK_HOME/conf/spark-defaults.conf
```
spark.driver.memory              4g
spark.executor.memory            4g
```

* Install Apache Toree/Jupyter Notebook to virtual environment (uses Python 3)
```bash
mkdir ~/dev
python -m venv ~/dev/jupyter
pip install --upgrade toree
sudo mkdir /usr/local/share/jupyter
sudo chown $USER /usr/local/share/jupyter
jupyter toree install --spark_home=$SPARK_HOME
pip install notebook
jupyter notebook
```
