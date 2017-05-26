package com.database;

import java.sql.*;

/**
 * Created by grantdeshazer on 4/29/17.
 */
public class DB {
    public Connection connection = null;
    private PreparedStatement _statement;

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

    public ResultSet queryDB(String sql) throws SQLException{
        _statement = connection.prepareStatement(sql);
        return _statement.executeQuery();
    }

    public boolean executeStatement(String sql) throws  SQLException{
        return connection.prepareStatement(sql).execute();
    }

    public void update(String sql) throws SQLException{
        _statement = connection.prepareStatement(sql);
        _statement.executeUpdate();
    }


    @Override
    protected void finalize() throws  Throwable {
        if (connection != null || !connection.isClosed()){
            connection.close();
        }
    }
}
