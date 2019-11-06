package com.pod.presto;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.facebook.presto.jdbc.PrestoStatement;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;


public class AppMain {
	
	public static void main(String[] args) {
		
		connect();
		
	}
	
	private static void connect(){
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
		try {
			Class.forName("com.facebook.presto.jdbc.PrestoDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		PrestoConnection connection = null;
		try {

//			connection = (PrestoConnection) DriverManager.getConnection("jdbc:presto://192.168.133.129:8090/tpch/sf1","tpch",null);
			connection = (PrestoConnection) DriverManager.getConnection("jdbc:presto://192.168.133.128:8183/tpch/sf1","tpch",null);

			PrestoStatement stmt = (PrestoStatement) connection.createStatement();

			// select * from userinfos;
			PrestoResultSet rs = (PrestoResultSet) stmt.executeQuery("show tables");  //select count(*) from lineitem   show tables
			
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
			rs.close();

//			rs = (PrestoResultSet) stmt.executeQuery("select count(*) from lineitem");
//			while (rs.next()) {
//				System.out.println(rs.getString(1));
//			}
			rs.close();
			connection.close();
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		
	}

}
