spark-mail
==========

# Overview
The Spark Mail project contains code for a tutorial on how to use Apache Spark to analyze email data. That data comes from two different sources:

1. The Enron Email dataset from Carnegie Mellon University (file-based)
1. We had planned on using public domain emails released by [Jeb Bush from his time as Florida governor PST files](http://www.jebemails.com/email/search). However, these data sets were retracted due to [PII](http://en.wikipedia.org/wiki/Personally_identifiable_information) concerns as described in [Jeb Bush Releases Personal Data(http://thinkprogress.org/election/2015/02/10/3621569/oops-jeb-bush-releases-personal-data-publishing-emails-interest-transparency/).

Tutorial on parsing Enron email to Avro and then explore the email set using Spark.

# Building the project
The Spark Mail project uses a Maven build. In order to avoid PermGen error (if using Java version < Java 8),
add the following to your .bashrc/environment:

```
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

Compile times can be dramatically reduced by using the [Zinc Scala build server](https://github.com/typesafehub/zinc).
Short overview and additional links at
[this blog entry](http://uebercomputing.com/scala/2014/11/09/Incremental-Compilation-With-Zinc/).

19MAR15: Exploring Spark SQL data frames and using very recently published com.databricks/spark-avro_2.10/1.0.0.

See https://github.com/databricks/spark-avro. To build to local Maven repo:

    sbt publishM2


# ETL (Extract Transform Load)
Both original datasets do not lend themselves to scalable processing. The Enron file set has over 500,000 files. Especially, when processing them with the Hadoop default FileInputFormat we would create over 500,000 input splits. Furthermore, we don't want our analytic code to have to deal with the parsing.

Therefore we parse the input once and aggregate the emails into the following [MailRecord format in Avro IDL](https://github.com/medale/spark-mail/blob/master/mailrecord/src/main/avro/com/uebercomputing/mailrecord/MailRecord.avdl):

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

# Enron Email Dataset

## Note to Windows Users
The Enron email dataset used (see below) contains files that end with a dot
(e.g. enron_mail_20110402/maildir/lay-k/inbox/1.).

The unit tests used actual emails from this dataset. This caused problems for
using Git from Eclipse. Checking the source code out from command line git
appeared to work.

However, the original code used the File classes listFiles() to do directory listings.
This also suffered from the problem that at least some versions of Windows
report files of the form FILE_NAME. (i.e. ending with dot) as just FILE_NAME
(no dot at the end). The unit tests failed on Windows. An attempt to fix this
by [using java.nio.file APIs instead](https://github.com/medale/spark-mail/issues/4)
also did not work.

### Workaround:
Renamed the test files with a .txt extension. That fixes the unit tests.
However, to process the actual files in the Enron dataset (see below) we need
to rename each file with a .txt extension. Note: Don't use dots as the end of
a file name!!!

## Obtaining/preparing the Enron dataset

https://www.cs.cmu.edu/~./enron/ describes the Enron email dataset and provides
a download link at https://www.cs.cmu.edu/~./enron/enron_mail_20150507.tgz. 

This email set is a gzipped tar file of emails stored in directories. Once
downloaded, extract via:
    > tar xfz enron_mail_20150507.tgz   (or use tar xf as new tar autodectects compression)

This generates the following directory structure:
* enron_mail_20150507
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

### Parsing Enron Email set into Apache Avro binary format

This data set at 423MB compressed is small but using the default small files
format to process this via FileInputFormat creates over 500,000 splits to be
processed. By doing some preprocesing and storing all the file artifacts in
Apache Avro records we can make the analytics processing more effective.

We parse out specific headers like Message-ID (uuid), From (from) etc. and store
all the other headers in a mailFields map. We also store the body in its own
field.

The [mailrecord-utils mailparser enronfiles Main class](https://github.com/medale/spark-mail/blob/master/mailrecord-utils/src/main/scala/com/uebercomputing/mailparser/enronfiles/AvroMain.scala)
allows us to convert the directory/file-based Enron data set into one Avro files
with all the corresponding MailRecord Avro records. To run this class from the
spark-mail root directory after doing a mvn clean install:

```
java -cp mailrecord-utils/target/mailrecord-utils-1.2.0-SNAPSHOT-shaded.jar \
com.uebercomputing.mailparser.enronfiles.AvroMain \
--mailDir /opt/local/datasets/enron/enron_mail_20150507/maildir \
--avroOutput /opt/local/datasets/enron/mail-2015.avro
```

# Spark Analytics
See spark-mail/analytics module source and test (in progress...)

# Enron Email As PST
* http://info.nuix.com/Enron.html

Note: Used pst file of bill_rapp.zip downloaded from nuix.com for testing.

# Public domain emails from Jeb Bush's time as Florida governor

## Background:
On December 13, 2014, [the Washington Post reported](http://www.washingtonpost.com/blogs/post-politics/wp/2014/12/13/jeb-bush-to-write-e-book-and-release-250000-e-mails/) that former Florida governor Jeb Bush plans to release over 250,000 emails from his time as Florida governor.

The American Bridge PAC had [requested the emails via public records request](http://americanbridgepac.org/happy-holidays-here-are-thousands-of-jeb-bushs-emails/) and listed the links below at http://americanbridgepac.org/jeb-bushs-gubernatorial-email-archive/. [Jeb Bush's website had the raw PST files also](http://www.jebemails.com/email/search). However due to PII and malware concerns, the raw PST files were retracted.

See [PST Processing Page](PstProcessing.md) for next steps.
