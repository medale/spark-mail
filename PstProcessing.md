# Processing PST Files

1. Download all links from http://americanbridgepac.org/jeb-bushs-gubernatorial-email-archive/ and put them in a local directory we will call $PSTS_HOME (in the examples below as /opt/rpm1/jebbush).

See sizes for original PST files:

     ls -lhr --sort=size *.pst


## Evaluating different roll-up/aggregation strategies

When we try to parallelize PST processing, each original PST file will likely be processed separately. To avoid corrupting Avro files, we use the original PST file name (with a .avro extension) to make the Avro files for each PST file distinct from each other even if the roll-up dates overlap.

### Roll-up by Day

```
cd spark-mail
java -classpath pst-utils/target/pst-utils-0.9.0-SNAPSHOT-shaded.jar com.uebercomputing.pst.Main --pstDir /opt/rpm1/jebbush --avroOutDir /opt/rpm1/jebbush/avro-daily --rollup daily > msg.txt 2>&1
```

Explore sizes of the newly created Avro files:

    cd /opt/rpm1/jebbush/avro-daily
    find . -name *.avro | xargs du | sort -n > daily-files.sizes

Results: Over 9000 avro files sizes 4K - just under 60MB. Too many small files.

### Roll-up by Month

```
cd spark-mail
java -classpath pst-utils/target/pst-utils-0.9.0-SNAPSHOT-shaded.jar com.uebercomputing.pst.Main --pstDir /opt/rpm1/jebbush --avroOutDir /opt/rpm1/jebbush/avro-monthly --rollup monthly > msg.txt 2>&1
```

Explore sizes of the newly created Avro files:

    cd /opt/rpm1/jebbush/avro-monthly
    find . -name *.avro | xargs du | sort -n > monthly-files.sizes

Results: 452 files. Smallest still 4K but largest over 320MB (will be split in blocks on Hadoop).

### Roll-up by Year

```
cd spark-mail
java -classpath pst-utils/target/pst-utils-0.9.0-SNAPSHOT-shaded.jar com.uebercomputing.pst.Main --pstDir /opt/rpm1/jebbush --avroOutDir /opt/rpm1/jebbush/avro-yearly --rollup yearly  > msg.txt 2>&1
```

Explore sizes:

    cd /opt/rpm1/jebbush/avro-yearly/
    find . -name *.avro | xargs du | sort -n > yearly-files.sizes

Results: 99 files (multiple writers for same year with different pst file and therefore avro names). Smallest still 4K but largest over 900MB. Large files will be split into blocks on HDFS but not when processing locally.

## Best Roll-up

As always in software, it depends. Using directory structure we can filter/index which records we want to explore. If we know specific dates we can easily hone in on individual days in the the daily roll-up. If we always process everything in Hadoop, then yearly is very efficient. For Hadoop and local processing monthly might be best. Need to explore and see where the bottlenecks are.
