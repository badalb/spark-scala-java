package com.badalb.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleSelectGlobalView {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Simple Select").master("local").getOrCreate();

		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("geo", DataTypes.StringType, true),
						DataTypes.createStructField("yr1980", DataTypes.DoubleType, true) });

		Dataset<Row> ds = spark.read().format("csv").option("header", true).schema(schema)
				.load("src/main/resources/data/populationbycountry19802010millions.csv");

		ds.createOrReplaceGlobalTempView("geodata");

		Dataset<Row> slightBiggerCountries = spark
				.sql("SELECT * from global_temp.geodata WHERE yr1980 > 1 ORDER BY 2 LIMIT 5");
		slightBiggerCountries.show();

	}

}
