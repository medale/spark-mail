% Apache Spark Through Email
% Markus Dale
% Nov 2018

# Intro, Slides And Code
* Slides: https://github.com/medale/spark-mail/blob/master/presentation/ApacheSparkThroughEmail.pdf
* Spark Code Examples: https://github.com/medale/spark-mail/
     * README.md describes how to get and parse Enron email dataset


# Data Science for Small Dataset

![Laptop](graphics/Laptop.png)


# Data Science for Larger Dataset

![Standalone Server](graphics/StandaloneServer1.png)


# Data Science for Larger Dataset (Vertical Scaling)

![Beefed-up Server](graphics/VerticalScaling.png)


# Data Science for Large Datasets (Horizontal Scaling)

![Multiple cooperating Servers](graphics/HorizontalScaling.png)


# Big Data Framework - Apache Hadoop

![HDFS, MapReduce](graphics/Hadoop.png)


# Hadoop Ecosystem

![Some Frameworks Around Hadoop](graphics/HadoopEcosystem.png)


# Apache Spark Components

![](graphics/SparkComponents.png)
\tiny Source: Spark: The Definitive Guide


# Hello, Spark Email World!
* Jupyter Notebook with Apache Toree
* See [Notebook ../notebooks/html/ApacheSparkThroughEmail1.html](../notebooks/html/ApacheSparkThroughEmail1.html)


# Cluster Manager, Driver, Executors, Tasks

![](graphics/SparkApplication.png)
\tiny Source: Apache Spark website


# SparkSession: Entry to cluster
* `spark`: [spark.sql.SparkSession](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession)

```scala
//SparkSession provided by notebook as spark
val records = spark.read.
   parquet("/datasets/enron/enron-small.parquet")

//In regular code for spark-submit 
//com.uebercomputing.spark.dataset.TopNEmailMessageSenders
val spark = SparkSession.builder().
   appName("TopNEmailMessageSenders").
   master("local[2]").getOrCreate()
```

# DataFrameReader: Input for structured data
* `spark.read`: [spark.sql.DataFrameReader](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader)
     * jdbc
     * json
     * parquet
     * text...
     * Also: https://spark-packages.org - Avro, Redshift, MongoDB...


# Scaling Behind the Scenes

![Jobs and Tasks](graphics/SparkJobsNotebook1.png)


# Stages: Pipeline work per stage - shuffle

![Stages](graphics/Notebook1Job2Dag.png)


# Where clause, Column methods, Built-in functions

* See [Notebook ../notebooks/html/ApacheSparkThroughEmail2.html](../notebooks/html/ApacheSparkThroughEmail2.html)


# Parallelism and Partitioning
* Goldilocks - not too many, not too few
* Initial parallelism - number of input "blocks"
* Shuffle - `spark.sql.shuffle.partitions` configuration


# Explode, Shuffle Partitions, UDF, Parquet partition

*  See [Notebook ../notebooks/html/ApacheSparkThroughEmail3.html](../notebooks/html/ApacheSparkThroughEmail3.html)


# And now for something completely different: Colon Cancer
* Screening saves lives! ![](graphics/Chemo.png){width=100px}
     * Colonoscopy - talk to your doc
* [Colorectal Cancer Alliance](https://www.ccalliance.org/)


# Questions?

![medale@asymmetrik.com](graphics/AsymmetrikPingPong.png){width=100px}

* [Baltimore Scala Meetup https://www.meetup.com/Baltimore-Scala/](https://www.meetup.com/Baltimore-Scala/)
* [Spark Mail repo https://github.com/medale/spark-mail/](https://github.com/medale/spark-mail)