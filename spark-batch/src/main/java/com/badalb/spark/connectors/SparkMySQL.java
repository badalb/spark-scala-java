package com.badalb.spark.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMySQL {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Spark Hive JDBC")
				.master("local")
				.getOrCreate();
		
		String driver = "com.mysql.cj.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true&useSSL=false";
		String user = "root";
		String pass = "helloworld";
		
		Dataset<Row> edf = spark.read().format("jdbc")
				.option("driver", driver)
				.option("url", url)
				.option("dbtable", "(select * from employee limit 5) emp")
				.option("user", user)
				.option("password", pass)
				.option("fetchsize", 100)
				.load();
		
		edf.printSchema();
		edf.show();

	}

}
