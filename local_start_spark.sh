spark-shell --master local[*] --driver-memory 4G --executor-memory 4G \
--jars sql-analytics/target/sql-analytics-1.1.0-SNAPSHOT-shaded.jar  \
--properties-file mailRecordKryo.conf \
--driver-java-options "-Dlog4j.configuration=log4j.properties"
