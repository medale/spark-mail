spark-shell --master local[*] --driver-memory 2G --executor-memory 2G \
--jars mailrecord-utils/target/mailrecord-utils-1.1.0-SNAPSHOT-shaded.jar \
--properties-file mailrecord-utils/mailrecord.conf \
--driver-java-options "-Dlog4j.configuration=log4j.properties"
