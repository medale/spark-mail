spark-shell --master local[*] --driver-memory 2G --executor-memory 2G \
--jars sql-analytics/target/sql-analytics-1.1.0-SNAPSHOT-shaded.jar  \
--properties-file mailRecordKryo.conf \
--driver-java-options "-Dlog4j.configuration=log4j.properties"
