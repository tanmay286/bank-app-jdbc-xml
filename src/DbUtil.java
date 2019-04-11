package com.capgemini.bankapp.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class DbUtil {

	private static String dburl;
	private static String username;
	private static String password;
	private static String className;

	static Connection connection;

	public static Connection getConnection(Properties properties) {
			
		className= properties.getProperty("classname");
		dburl = properties.getProperty("dburl");
		username = properties.getProperty("username");
		password = properties.getProperty("password");

		try {
			Class.forName(className);
			if (connection == null) {
				connection = DriverManager.getConnection(dburl, username, password);
				connection.setAutoCommit(false);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}

	public static void commit() {
		try {
			if (connection != null) {
				connection.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void rollback() {
		try {
			if (connection != null) {
				connection.rollback();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
