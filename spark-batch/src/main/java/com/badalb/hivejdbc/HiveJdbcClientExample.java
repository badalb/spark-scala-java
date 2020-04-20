package com.badalb.hivejdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClientExample {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hiveuser", "");
		Statement stmt = con.createStatement();

		String tableName = "empdata";
		stmt.execute("drop table " + tableName);
		stmt.execute("create table " + tableName + " (id int, name string, dept string)");

		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(2));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /home/user/input.txt is a ctrl-A separated file with three fields per
		// line
		String filepath = "/opt/foo.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);

		sql = "select * from empdata where id='1'";
		res = stmt.executeQuery(sql);
		// show tables
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
			System.out.println(res.getString(2));
			System.out.println(res.getString(3));
		}
		res.close();
		stmt.close();
		con.close();
	}
}