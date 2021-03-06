package com.pod.presto;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.facebook.presto.jdbc.PrestoStatement;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;

public class pg {

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

			connection = (PrestoConnection) DriverManager.getConnection("jdbc:presto://127.0.0.1:8080/postgresql/public","root",null);

            PrestoStatement stmt = (PrestoStatement) connection.createStatement();

            PrestoResultSet rs = null;
            rs = (PrestoResultSet) stmt.executeQuery("show tables");  //select count(*) from lineitem   show tables

			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
			rs.close();

            rs = (PrestoResultSet) stmt.executeQuery("select * from userinfos ");
            while (rs.next()) {
                System.out.println(rs.getInt(1)+"/"+rs.getString(2)+"/"+rs.getInt(3));
            }
            rs.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}
