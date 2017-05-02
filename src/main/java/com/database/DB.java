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

    public ResultSet replaceStringQuery(String sql, String replacement) throws SQLException{
        _statement = connection.prepareStatement(sql);
        _statement.setString(1 , replacement);
        return _statement.executeQuery();
    }

    public boolean replaceStringUpdate(String sql, String replacement) throws SQLException{
        _statement = connection.prepareStatement(sql);
        _statement.setString(1, replacement);
        return _statement.execute();
    }

    public boolean executeStatement(String sql) throws  SQLException{
        return connection.prepareStatement(sql).execute();
    }

    public void update(String sql) throws SQLException{
        _statement = connection.prepareStatement(sql);
        _statement.executeUpdate();
    }

    public void truncateTable(String table) {
        try {
            if(table.contains("streamcheck")) {
                connection.prepareStatement("TRUNCATE streamcheck RESTART IDENTITY ").execute();
            } else if (table.contains("record")){
                connection.prepareStatement("TRUNCATE record RESTART IDENTITY ").execute();
            } else {
                System.out.println("Invalid table to truncate :: " + table + " doesn't exist");
            }
        } catch (Exception e){
            e.printStackTrace();
            System.err.println("Failed to truncate table " + table);
        }
    }

    @Override
    protected void finalize() throws  Throwable {
        if (connection != null || !connection.isClosed()){
            connection.close();
        }
    }
}
