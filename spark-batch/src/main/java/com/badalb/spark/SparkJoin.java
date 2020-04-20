package com.badalb.spark;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJoin {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder()
				.appName("Spark SQL join example")
				.master("local")
				.getOrCreate();

		// higher education dataset
		String higherEduDataFile = "src/main/resources/data/InstitutionCampus.csv";
		
		Dataset<Row> heudDs = sparkSession
				.read()
				.format("csv")
				.option("inferSchema", "true").option("header", true)
				.load(higherEduDataFile);
		heudDs = heudDs.filter("LocationType='Institution'").withColumn("addressElements",
				split(heudDs.col("Address"), " "));

		heudDs = heudDs.withColumn("addressElementCount", size(heudDs.col("addressElements")));

		heudDs = heudDs.withColumn("zip9",
				element_at(heudDs.col("addressElements"), heudDs.col("addressElementCount")));
		heudDs = heudDs.withColumn("splitZipCode", split(heudDs.col("zip9"), "-"));

		heudDs = heudDs.withColumn("zip", heudDs.col("splitZipCode").getItem(0))
				.withColumnRenamed("LocationName", "Institute").drop("DapipId").drop("OpeId").drop("ParentName")
				.drop("ParentDapipId").drop("LocationType").drop("Address").drop("GeneralPhone").drop("AdminName")
				.drop("AdminPhone").drop("AdminEmail").drop("Fax").drop("UpdateDate").drop("zip9")
				.drop("addressElements").drop("addressElementCount").drop("splitZipCode");
		//heudDs.show(5);

		// county-zip adataset
		String countyZipData = "src/main/resources/data/COUNTY_ZIP_092018.csv";
		Dataset<Row> countyZipDs = sparkSession.read().format("csv").option("inferSchema", true).option("header", true)
				.load(countyZipData);

		countyZipDs = countyZipDs.drop("res_ratio").drop("bus_ratio").drop("oth_ratio").drop("tot_ratio");
		countyZipDs.show(5);

		// census dataset
		String censusDataFile = "src/main/resources/data/PEP_2017_PEPANNRES.csv";
		Dataset<Row> censusDs = sparkSession.read().format("csv").option("inferSchema", "true").option("header", true)
				.load(censusDataFile);

		censusDs = censusDs.drop("GEO.id").drop("rescen42010").drop("resbase42010").drop("respop72010")
				.drop("respop72011").drop("respop72012").drop("respop72013").drop("respop72014").drop("respop72015")
				.drop("respop72016").withColumnRenamed("respop72017", "pop72017")
				.withColumnRenamed("GEO.id2", "countyId").withColumnRenamed("GEO.display-label", "county");

//		censusDs.show(5);

		// Institute - County - Zip Code - Population
//		Dataset<Row> heducountyDs = heudDs.join(countyZipDs, heudDs.col("zip").equalTo(countyZipDs.col("zip")))
//				.join(censusDs, countyZipDs.col("county").equalTo(censusDs.col("countyId")))
//				.drop(countyZipDs.col("zip")).drop(countyZipDs.col("county")).drop(censusDs.col("countyId"))
//				.withColumnRenamed("pop72017", "population");
//		heducountyDs.show(10);

		// Top ten county with maximum number of institutes
		//Dataset<Row> topCounties = heducountyDs.groupBy("county").count()
		//		.orderBy(org.apache.spark.sql.functions.col("count").desc());
		//topCounties.show(10);
		
//		//zip code shared by county
		Dataset<Row> zipCodeCounty = countyZipDs.groupBy("county").count()
				.orderBy(org.apache.spark.sql.functions.col("count").desc());
		zipCodeCounty.show(5);
		
		
		
		
		

	}

}




