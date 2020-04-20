package com.badalb.spark.connectors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LocalTempTableCreator {

	public static void create(SparkSession spark) {

		List<File> files = listFiles("src/main/resources/csv");

		files.forEach(file -> {
			Dataset<Row> ds = spark.read().format("csv")
			.option("header", "true")
			.option("delimiter", ",")
			.option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
			.load(file.getAbsolutePath());
			
			int idx = file.getName().lastIndexOf(".");
			String name = file.getName().substring(0,idx);
			ds.createOrReplaceTempView(name);
		});
	}

	/**
	 * List all the files under a directory
	 * 
	 * @param directoryName to be listed
	 */
	public static List<File> listFiles(String directoryName) {
		List<File> files = new ArrayList<>();
		File directory = new File(directoryName);
		// get all the files from a directory
		File[] fList = directory.listFiles();
		for (File file : fList) {
			if (file.isFile()) {
				files.add(file);
			}
		}
		return files;
	}

}
