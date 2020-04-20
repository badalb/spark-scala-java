package com.badalb.spark.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHiveJdbc {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Hive JDBC").master("local")
				 .config("hive.metastore.uris", "thrift://localhost:9083")
				.enableHiveSupport().getOrCreate();

		Dataset<Row> edf = spark.read().format("jdbc")
				// .option("driver", "org.apache.hadoop.hive.jdbc.HiveDriver")
				.option("url", "jdbc:hive2://localhost:10000/")
				.option("dbtable", "(select * from employee limit 5) emp").option("user", "hiveuser")
				.option("password", "").option("fetchsize", 100).load();

		edf.printSchema();
		//edf.createOrReplaceTempView("emp");

		Dataset<Row> df = spark.sql("show tables");
		df.show();

		 df = spark.sql("select * from employee limit 5");
		df.show();
	}

}
