package com.badalb.hivejdbc;

import java.sql.SQLException;
import java.sql.Connection;

import java.sql.Statement;
import java.sql.DriverManager;

public class HiveCreateDb {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	   
	   public static void main(String[] args) throws SQLException {
			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(1);
			}
	 
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://localhost:10000/default", "hiveuser", "");
			Statement stmt = con.createStatement();
	      
	      stmt.executeQuery("CREATE DATABASE userdb");
	      
	      System.out.println("Database userdb created successfully.");
	      
	      con.close();
	   }
	}