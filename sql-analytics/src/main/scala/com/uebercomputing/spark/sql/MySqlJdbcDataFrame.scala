package com.uebercomputing.spark.sql

import java.util.Properties

/**
 * sudo apt-get install mysql-server
 * mysql -u root -p
 * CREATE USER 'spark'@'localhost' IDENTIFIED BY 'spark-rocks!';
 * CREATE DATABASE spark;
 * GRANT ALL PRIVILEGES ON spark.* TO 'spark'@'localhost';
 * flush privileges;
 */
object MySqlJdbcDataFrame {

  def main(args: Array[String]): Unit = {
    val sqlContext = org.apache.spark.sql.test.TestSQLContext

    val rolesDf = sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").load("roles.csv")

    //http://spark.apache.org/docs/latest/sql-programming-guide.html
    //JDBC To Other Databases
    val props = new Properties()
    props.setProperty("user", "spark")
    props.setProperty("password", "spark-rocks!")
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    val url = "jdbc:mysql://localhost:3306/spark"

    //If:
    //java.sql.SQLException: No suitable driver found for jdbc:mysql://localhost:3306/spark
    //Then: SPARK_CLASSPATH=mysql-connector-java-5.1.35.jar spark-shell...
    rolesDf.write.mode("overwrite").jdbc(url, "roles", props)

  }
}
