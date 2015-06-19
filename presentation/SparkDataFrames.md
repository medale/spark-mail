% Apache Spark DataFrames
% Markus Dale
% 2015

# Spark Ecosystem

![Databricks Spark 1.4.0 @ecosystem_databricks_2015](graphics/SparkComponents-Databricks-2015-06-18.png)

# Spark SQL

* Structured/semi-structured data on Spark
* Can write SQL-like queries or
* DataFrames DSL language
* Michael Armbrust (Databricks Spark SQL lead):

    * Write less code
    * Read less data
    * Let [Catalyst query] optimizer do the hard work

# Spark SQL in Context

* Complete re-write/superset of Shark announced April 2014
* Not [Hive on Spark](https://issues.apache.org/jira/browse/HIVE-7292)
* Leverages Spark Core infrastructure/RDD abstractions
* Separate library (in addition to Spark Core): spark-sql, spark-hive

# DataFrame

* Introduced in Spark 1.3 March 2015 (presentation uses 1.4.0)
* Replacement/evolution of SchemaRDD
* Inspired by data frames in [Python Data Analysis (pandas)](http://pandas.pydata.org/) and
[R](http://www.r-project.org/)
* Distributed collection of Row objects (with known schema/columns)
* Abstractions for selecting, filtering, aggregation

# Catalyst Query Optimizer Pipeline

![Catalyst Query Optimizer Pipeline @armbrust_whats_2015](graphics/SparkSql-Catalyst-Databricks-2015-03-24.png)

# DataFrame Speed Up - Catalyst Query Optimizer

![DataFrame Runtimes @armbrust_beyond_2015](graphics/DataFrameSpeed-MichaelArmbrust-2015.png)

# Spark SQL Data Sources

![Internal and external data sources @armbrust_whats_2015](graphics/SparkSql-DataSourcesApi-Databricks-2015-03-24.png)

# Apache Parquet

* Columnar storage format - store data by chunks of columns rather than rows
* Support complex nesting using algorithms from [Google Dremel @melnik_dremel_2010].
* See [Apache Parquet docs @parquet_apache_2014]

# Parquet File Structure

![Parquet File Structure @white_hadoop_2015](graphics/Parquet-HadoopDefinitive-2015.png)

# References {.allowframebreaks}
