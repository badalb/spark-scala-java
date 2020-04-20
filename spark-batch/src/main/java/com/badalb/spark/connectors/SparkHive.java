package com.badalb.spark.connectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHive {

	public static void main(String[] rags) {
		SparkSession spark = SparkSession.builder()
				.appName("Spark Hive JDBC")
				.master("local")
				.getOrCreate();
		
		LocalTempTableCreator.create(spark);
		
		Dataset<Row> df= spark.sql("select * from employee");
		df.show();
		
	}
}
