% Apache Spark Through Email
% Markus Dale
% Nov 2018

# Slides And Code
* Slides: https://github.com/medale/spark-mail/blob/master/presentation/ApacheSparkThroughEmail.pdf
* Spark Code Examples: https://github.com/medale/spark-mail/


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


# Hello, Email World!
* Jupyter Notebook with Apache Toree
* See [ApacheSparkThroughEmail1](../notebooks/html/ApacheSparkThroughEmail1.html)


# A Spark Application - Driver, Executors, Tasks, Cluster Managers

![](graphics/SparkApplication.png)
\tiny Source: Spark: The Definitive Guide


# DataFrameReader: Built-in Data Formats
* [spark.sql.SparkSession](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession)
* [spark.sql.DataFrameReader](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader)
     * jdbc
     * json
     * parquet
     * text...
     * Also: https://spark-packages.org - Avro, Redshift, MongoDB...


# Scaling Behind the Scenes

![Jobs and Tasks](graphics/SparkJobsNotebook1.png)


# Combining work per stage - shuffle

![Stages](graphics/Notebook1Job2Dag.png)


# Where clause, Column methods, Built-in functions

* See [Apache Spark Through Email Notebook 2](../notebooks/html/ApacheSparkThroughEmail2.html)


# Parallelism and Partitioning

* Initial parallelism - number of input "blocks"
* Shuffle - `spark.`


# Colon Cancer
* Screening saves lives! ![](graphics/Chemo.png){width=100px}
     * Colonoscopy - talk to your doc
* [Colorectal Cancer Alliance](https://www.ccalliance.org/)


# Questions?

![medale@asymmetrik.com](graphics/AsymmetrikPingPong.png)