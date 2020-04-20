package com.badalb.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

public class PersistenceCheck {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Simple Select").master("local").getOrCreate();
		scala.collection.Map<Object, RDD<?>> rdds = spark.sparkContext().getPersistentRDDs();
		System.out.println(rdds);
	}

}
