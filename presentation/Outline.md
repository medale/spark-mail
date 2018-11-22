# Data Science for Big Datasets
* vertical scaling - big iron (graphics)
* horizontal scaling - parallel/distributed computing (graphics)

# Lineage
* Google File System, Google MapReduce papers

# Apache Hadoop
* Open source - Hadoop HDFS, MapReduce

# Apache Hadoop Failure Redundancy
* Failure redundancy - disk

# Apache Spark
* Resilient Distributed Datasets (RDDs)
* Cache in memory

# Apache Spark Failure Redundancy
* Lineage/transformations back to source - recompute

# Cluster Managers

# Getting Faster
* Java Objects - large
* Kryo serialization for data exchange/shuffle operations
* Garbage collection - slow
* Managed memory storage - DataFrames and Datasets

# Datasets
* Sources, typing

# Partitioning
* original - from source (e.g. HDFS blocks)

# Transformations vs. Actions
* Lazy evaluation

# GUI

# Batch

# Shell

# Streaming

# API

# Colon Cancer

# Miscellaneous - to incorporate
```
docker pull jupyter/all-spark-notebook

# Must match Spark version on notebook - see https://github.com/jupyter/docker-stacks/tree/master/all-spark-notebook
$SPARK_HOME/sbin/start-master.sh --host 192.168.1.15
$SPARK_HOME/sbin/start-slave.sh spark://192.168.1.15:7077

docker run -p 8888:8888 -v /home/medale/datasets/enron/:/home/medale/datasets/enron --net=host \
   --pid=host -e TINI_SUBREAPER=true \
   -e SPARK_OPTS='--master=spark://192.168.1.15:7077 --executor-memory=8g' \
   jupyter/all-spark-notebook
```
