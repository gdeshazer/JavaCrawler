package com.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by grantdeshazer on 4/29/17.
 */
public class DB {
    public Connection connection = null;

    public DB(){
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/crawler");
        } catch (Exception e){
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened crawler successfully");
    }

    public DB(String databaseName) {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened " + databaseName + " successfully");
    }

    public ResultSet runSql(String sql) throws SQLException{
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    public boolean runSql2(String sql) throws  SQLException{
        Statement statement = connection.createStatement();
        return statement.execute(sql);
    }

    public void update(String sql) throws SQLException{
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    @Override
    protected void finalize() throws  Throwable {
        if (connection != null || !connection.isClosed()){
            connection.close();
        }
    }
}
