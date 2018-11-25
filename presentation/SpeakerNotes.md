% Apache Spark Through Email
% Markus Dale
% Nov 2018

# Intro, Slides And Code
* mostly Java, big data with Hadoop
* big data with Spark, Databricks, Scala
* Now Asymmetrik - Scala, Spark, Elasticsearch, Akka...
* Slides: https://github.com/medale/spark-mail/blob/master/presentation/ApacheSparkThroughEmail.pdf
* Spark Code Examples: https://github.com/medale/spark-mail/


# Data Science for Small Dataset
* Laptop
* Explore subset, develop approaches/algorithms, find *features*


# Data Science for Larger Dataset
* Standalone server - more memory, faster CPU, more storage


# Data Science for Larger Dataset (Vertical Scaling)
* Big iron - lots of cores, memory, disk/SSDs, GPUs


# Data Science for Large Datasets (Horizontal Scaling)
* Parallelize, coordinate compute among many "commodity" machines
* Deal with *failure*


# Big Data Framework - Apache Hadoop
* Google GFS (2003), Google MapReduce (2004)
* Hadoop (Nutch - open source web crawler/Lucene) - Doug Cutting, Mike Cafarella
     * Yahoo, Cloudera, Hortonworks, MapR


# Hadoop Ecosystem
* HDFS, YARN, MapReduce (Spark replaces MR)
* HBase (Google BigTable), Cassandra, Accumulo
* Pig, Hive - MR scripting DSL/SQL


# Apache Spark Components
* Foundation: Resilient Distributed Datasets (RDD)
     * Broadcast variables, accumulators
     * Java objects, should use Kryo serialization
* *Structured APIs* (use these) - Datasets, DataFrames, SQL
     * Spark manages object layout in memory, schemas, code generation
* Streaming, MLlib (Advanced analytics)
* Scala, Java, Python, R + library ecosystems 
* Submit (Batch/Stream) or Shell/Notebooks (e.g. Zeppelin, Jupyter)


# Hello, Spark Email World!
* Jupyter Notebook with Apache Toree
* See [ApacheSparkThroughEmail1](../notebooks/html/ApacheSparkThroughEmail1.html)


# Cluster Manager, Driver, Executors, Tasks

* Cluster manager: Spark Standalone, Hadoop YARN, AWS EMR
* Driver (start once)
     * Execute user code
     * Schedules tasks for executors
     * Serialize code (closures with data) as tasks to executors
* Executors on worker nodes (start once, restart)
     * Cache - distributed memory for partitions
     * Execute tasks
     * Read/manage partitions (serialization - Kryo)