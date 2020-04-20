package com.badalb.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleSelect {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Simple Select").master("local").getOrCreate();

		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("geo", DataTypes.StringType, true),
						DataTypes.createStructField("yr1980", DataTypes.DoubleType, true) });

		Dataset<Row> ds = spark.read().format("csv").option("header", true).schema(schema)
				.load("src/main/resources/data/populationbycountry19802010millions.csv");

		ds.createOrReplaceTempView("geodata");

		Dataset<Row> smallCountries = spark.sql("SELECT * from geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");
		
		smallCountries.cache();

		scala.collection.Map<Object, RDD<?>> rdds =    spark.sparkContext().getPersistentRDDs();
		
		
		System.out.println(rdds.values());
		smallCountries.show();

	}

}
