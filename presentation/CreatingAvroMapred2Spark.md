# Spark 1.2.1-rc3 with Avro-mapred-1.7.6-hadoop2

Due to [SPARK-3039 Wrong Avro Mapred Library Version (hadoop1 instead of hadoop2)](https://issues.apache.org/jira/browse/SPARK-3039)
we created our own version of Avro with a fix for that problem. This fix has been
submitted as the [Apache Spark Pull Request 4315 on GitHub](https://github.com/apache/spark/pull/4315)
and is included in the master (1.3.0-SNAPSHOT) and 1.3.0 release candidates.
Hopefully, it will also be backported to 1.2.2 onwards. But here is how
to build a release using the v1.2.1 release.

## Obtaining Git repo for Apache Spark

Apache Spark's GitHub repo is at https://github.com/apache/spark/.

    git clone git@github.com:apache/spark.git
    cd spark
    # Create branch fix from tag v1.2.1 (latest release branch)
    git checkout -b fix v1.2.1

## Fixing SPARK-3039

The fix consists of explicitly excluding the bad version of avro-mapred-1.7.5.jar:

In sql\hive\pom.xml:

### Add explicit exclusion of bad version
```
    <dependency>
      <groupId>org.spark-project.hive</groupId>
      <artifactId>hive-exec</artifactId>
      <version>${hive.version}</version>
      <exclusions>
        <exclusion>
          <groupId>commons-logging</groupId>
          <artifactId>commons-logging</artifactId>
        </exclusion>
        <exclusion>
          <groupId>com.esotericsoftware.kryo</groupId>
          <artifactId>kryo</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.apache.avro</groupId>
          <artifactId>avro-mapred</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

We just added the exclusion for avro-mapred. commons-logging and kryo were already
excluded.

### Later on in pom.xml, explicit version of avro-mapred is already defined

```
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro-mapred</artifactId>
      <version>${avro.version}</version>
      <classifier>${avro.mapred.classifier}</classifier>
    </dependency>
```

## Make Distribution with Hive, Yarn and Hadoop 2.4

See [Building Spark](http://spark.apache.org/docs/1.2.0/building-spark.html)

    ./make-distribution.sh -Pyarn -Phive -Phadoop-2.4 -Phive-0.13.1
    cp -R dist /usr/local/spark-1.2.1-hadoop2.4
    cd /usr/local
    ln -s spark-1.2.1-hadoop2.4 spark

## Add Spark binaries to Path

    vi ~/.bashrc
    export JAVA_HOME=/usr/local/java
    export SPARK_HOME=/usr/local/spark
    export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

    # test
    which spark-shell
    > /usr/local/spark/bin/spark-shell
    which spark-submit
    > /usr/local/spark/bin/spark-submit

## Setting Default Configurations

    cd /usr/local/spark/conf
    mv spark-env.sh.template spark-env.sh

    # Ensure file is executable
    ls -l spark-env.sh
    > -rwxr-xr-x 1 ... spark-env.sh

    vi spark-env.sh
    HADOOP_CONF_DIR=

## Testing the New Installation

See SparkMailFromSparkShell.md
