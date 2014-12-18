spark-mail
==========

Tutorial on parsing Enron email to Avro and then explore the email set using Spark.

# Note to Windows Users
The Enron email dataset used (see below) contains files that end with a dot
(e.g. enron_mail_20110402/maildir/lay-k/inbox/1.). The unit tests use actual
emails from this dataset and those files also end with a dot
(e.g. mailrecord-utils/src/test/resources/enron/maildir/lay-k/inbox/568.).
This seems to cause problems for using Git from Eclipse. Checking the
source code out from command line git appeared to work.

The original code used the File classes listFiles() to do directory listings.
This also suffered from the problem that at least some versions of Windows
report files of the form FILE_NAME. (i.e. ending with dot) as just FILE_NAME
(no dot at the end). Attempting to fix this
[using java.nio.file APIs instead](https://github.com/medale/spark-mail/issues/4).


# Obtaining/preparing the Enron dataset

https://www.cs.cmu.edu/~./enron/ describes the Enron email dataset and provides
a download link at https://www.cs.cmu.edu/~./enron/enron_mail_20110402.tgz

This email set is a gzipped tar file of emails stored in directories. Once
downloaded, extract via:
    > tar xfz enron_mail_20110402.tgz   (or use tar xf as new tar autodectects compression)

This generates the following directory structure:
* enron_mail_20110402
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

## Parsing Enron Email set into Apache Avro binary format

This data set at 423MB compressed is small but using the default small files
format to process this via FileInputFormat creates over 500,000 splits to be
processed. By doing some preprocesing and storing all the file artifacts in
Apache Avro records we can make the analytics processing more effective. We
will use the following [MailRecord format in Avro IDL](https://github.com/medale/spark-mail/blob/master/mailrecord/src/main/avro/com/uebercomputing/mailrecord/MailRecord.avdl):

```
@version("1.0.0")
@namespace("com.uebercomputing.mailrecord")
protocol MailRecordProtocol {
  record MailRecord {
    string uuid;
    string from;
    union{array<string>,null} to = null;
    union{array<string>,null} cc = null;
    union{array<string>,null} bcc = null;
    long dateUtcEpoch;
    string subject;
    union{map<string>,null} mailFields = null;
    string body;
  }
}
```

We parse out specific headers like Message-ID (uuid), From (from) etc. and store
all the other headers in a mailFields map. We also store the body in its own
field.

The [mailrecord-utils mailparser Main class](https://github.com/medale/spark-mail/blob/master/mailrecord-utils/src/main/scala/com/uebercomputing/mailparser/Main.scala)
allows us to convert the directory/file-based Enron data set into one Avro files
with all the corresponding MailRecord Avro records.

# Spark Analytics
See spark-mail/analytics module source and test (in progress...)
