package com.badalb.spark.connectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetJava {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
				.appName("Spark Hive JDBC")
				.master("local")
				.getOrCreate();

		Dataset<Row> peopleDF = spark.read().json("src/main/resources/people.json");
		peopleDF.show();

		// DataFrames can be saved as Parquet files, maintaining the schema information
		peopleDF.write().parquet("people.parquet");

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

		// Parquet files can also be used to create a temporary view and then used in
		// SQL statements
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0),
				Encoders.STRING());
		namesDS.show();
	}

}
