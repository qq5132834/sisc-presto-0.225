package com.facebook.presto.jdbc;


import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;

/***
 *
 */
public class ZUK_JdbcPreparedStatement {


//    @Test
//    public void test1() throws Exception {
//        TestingPrestoServer server = new TestingPrestoServer();
//        String url = format("jdbc:presto://%s", server.getAddress());
//        Connection connection = DriverManager.getConnection(url, "test", null);
//        PrestoStatement stmt = (PrestoStatement) connection.createStatement();
//        PrestoResultSet rs = null;
//        rs = (PrestoResultSet) stmt.executeQuery("SELECT 123 x");
//
//        while (rs.next()) {
//            System.out.println(rs.getString(1));
//        }
//        rs.close();
//        stmt.close();
//        connection.close();
//    }



    @Test
    public void test() throws SQLException {

        String catalog = "mysql";
        String schema = "baseinfo";
        String url = format("jdbc:presto://%s/%s/%s", "192.168.133.128:8183", catalog, schema);
        System.out.println(url);
        Connection connection = DriverManager.getConnection(url, "test", null);

        System.out.println(connection.getClass().getName());

        PrestoStatement stmt = (PrestoStatement) connection.createStatement();
        PrestoResultSet rs = null;
        rs = (PrestoResultSet) stmt.executeQuery("show tables");

        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        stmt.close();
        connection.close();

    }

}
