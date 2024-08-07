% Apache Spark Through Email
% Markus Dale, medale@asymmetrik.com
% July 2024

# Intro, Slides And Code
* Slides: https://github.com/medale/spark-mail/blob/master/presentation/ApacheSparkThroughEmail.pdf
* Spark Code Examples: https://github.com/medale/spark-mail/
     * README.md describes how to get and parse Enron email dataset


# Goals

![Intro to Apache Spark](graphics/Goal.png)


# Data Science for Small Dataset/Exploratory Data Analysis (EDA)

![Laptop](graphics/Laptop.png)

# Data Science for Larger Dataset (Vertical Scaling)

![Beefed-up Server](graphics/VerticalScaling.png){height=80%}


# Data Science for Large Datasets (Horizontal Scaling)

![Multiple cooperating Servers](graphics/HorizontalScaling.png)


# Early Big Data Framework - Apache Hadoop

![HDFS, MapReduce](graphics/Hadoop.png)


# Hadoop Ecosystem

![Some Frameworks Around Hadoop](graphics/HadoopEcosystem.png)

# Running Spark
* Local
     * Download from https://spark.apache.org, untar, add to PATH
     * SDKMAN - `curl -s "https://get.sdkman.io" | bash`
          * `sdk install spark`
     * `spark-shell` or `pyspark`
     * Edit `$SPARK_HOME/conf/spark-defaults.conf` (from template)
          * `spark.driver.memory              8g` 
* Standalone cluster, Hadoop YARN
     * Need shared file system or common datastore (e.g. AWS S3) 
* Cloud-based managed:
     * AWS EMR
     * GCP Dataproc
     * Databricks on Azure, GCP or AWS
  

# Apache Spark Components

![](graphics/SparkComponents.png)
\tiny Source: Spark: The Definitive Guide


# Hello, Spark Email World!
```bash
spark-shell --master "local[4]" --driver-memory 8G
```
* Jupyter notebook with Apache Toree [Notebook ../notebooks/html/ApacheSparkThroughEmail1.html](https://medale.github.io/spark-mail/notebooks/html/ApacheSparkThroughEmail1.html)

# Cluster Manager, Driver, Executors, (Jobs -> Tasks)

![](graphics/SparkApplication.png)
\tiny Source: Apache Spark website


# SparkSession: Entry to cluster
* `spark`: [spark.sql.SparkSession](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession)

```scala
//SparkSession provided by notebook or shell as spark
val homeDir = sys.props("user.home")
val records = spark.read.
   parquet(s"$homeDir/datasets/enron/enron-small.parquet")

//In regular code for spark-submit 
//com.uebercomputing.spark.dataset.TopNEmailMessageSenders
val spark = SparkSession.builder().
   appName("TopNEmailMessageSenders").
   master("local[2]").getOrCreate()
```

# DataFrameReader/Writer: Input/Output for structured data
* `spark.read/write`: [spark.sql.DataFrameReader/Writer](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader)
     * jdbc
     * json
     * parquet
     * text...
     * Also: https://spark-packages.org - Redshift, MongoDB...

# Convert Dataset Format
\scriptsize
```scala
import org.apache.spark.sql.functions._
val homeDir = sys.props("user.home")
val records = spark.read.parquet(s"$homeDir/datasets/enron/enron-small.parquet")

// write block-size file(s)
records.write.json(s"$homeDir/datasets/enron/json-parts")
records.repartition(1).write.json(s"$homeDir/datasets/enron/json-single")

// Dataset has mailfields/RE: and mailfields/re: fields
spark.conf.set('spark.sql.caseSensitive', true)
val jsonIn = spark.read.json(s"$homeDir/datasets/enron/json-parts")
```

# Transformations vs. Actions
* Transformation: returns a new RDD (nothing gets executed)
     * `read`, `cache`, `select`, `where`...
* Actions: trigger execution, catalyst query optimizer, Tungsten code generation
     * `count`
     * Bring rows back to driver: `take`, `collect` (watch OOM!)
     * `write`

# Unified Language Interface via Catalyst
![](graphics/CatalystUnifiedInterface.png)

# Phases of Catalyst Query Planning
![](graphics/QueryPlanningPhases.png)

# Scaling Behind the Scenes

![Jobs and Tasks](graphics/SparkJobsNotebook1.png)


# Stages: Pipeline work per stage - shuffle

![Stages](graphics/Notebook1Job2Dag.png){height=80%}


# Where clause, Column methods, Built-in functions

* See [Notebook ../notebooks/html/ApacheSparkThroughEmail2.html](https://medale.github.io/spark-mail/notebooks/html/ApacheSparkThroughEmail2.html)


# Spark APIs - DataFrameReader, Dataset, Column, functions

![](graphics/DataFrameReader.png){height=40%}\ ![](graphics/DatasetApi.png){height=40%}
![](graphics/Column.png){height=40%}\ ![](graphics/FunctionApi.png){height=40%}


# Parallelism and Partitioning
* Goldilocks - not too many, not too few
* Initial parallelism - number of input "blocks"
* Splittable file formats (e.g. parquet, avro, bzip2)
     * Not zip, gzip! 
* Shuffle - Adaptive Query Execution (dynamic partitioning)


# Explode, Shuffle Partitions, UDF, Parquet partition

*  See [Notebook ../notebooks/html/ApacheSparkThroughEmail3.html](https://medale.github.io/spark-mail/notebooks/html/ApacheSparkThroughEmail3.html)


# Pandas on Spark
* See https://spark.apache.org/pandas-on-spark/


# Apache Parquet/Apache Arrow
* Avro - record-oriented data format
* Parquet - column-oriented data format by page
* Arrow - share memory for Python
* https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html


# Resources
* https://spark.apache.org/
* https://spark-packages.org/ - Community 3rd party packages (e.g. data sources)
* https://sparkbyexamples.com/
* RDD - https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf
* Spark SQL - https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
* Adaptive query execution - https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html



# And now for something completely different: Colon Cancer
* Screening saves lives! ![](graphics/Chemo.png){width=100px}
     * Colonoscopy - talk to your doc
* [Colorectal Cancer Alliance](https://www.ccalliance.org/)


# Questions?

![](graphics/Farley.png){width=100px}\ ![](graphics/AsymmetrikPingPong.png){width=100px}

* markus.dale@bluehalo.com
* [Infrequent blog/past presentations http://uebercomputing.com/](http://uebercomputing.com/)
* [Spark Mail repo https://github.com/medale/spark-mail/](https://github.com/medale/spark-mail)
