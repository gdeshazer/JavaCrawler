package com.database;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

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


    public void deleteFromDB(String url){
        PreparedStatement statement;

        try {
            statement = connection.prepareStatement("SELECT recordid, url FROM record WHERE url=?");
            statement.setString(1, url);
            ResultSet urlFromDB = statement.executeQuery();

            if(urlFromDB.next()){
                statement = connection.prepareStatement("DELETE FROM record WHERE recordid=?");
                statement.setInt(1, urlFromDB.getInt("recordid"));
                statement.execute();
            }

        } catch (SQLException e){
            System.err.println("Failed to query database");
            e.printStackTrace();
        }
    }


    public void addToBlacklist(String url){
        PreparedStatement statement;

        try {
            URI uri = new URI(url);
            String domain = uri.getHost();

            statement = connection.prepareStatement("INSERT INTO blacklist (url) VALUES (?), (?)");
            statement.setString(1, url);
            statement.setString(2, domain);
            statement.execute();


        } catch (URISyntaxException e){
            System.err.println("Incorrect URI format - can't add domain to blacklist");
        } catch (SQLException e){
            System.err.println("Failed to query database");
            e.printStackTrace();
        }
    }

    public void resetSequence(){
        try {

            PreparedStatement statement1 = connection.prepareStatement("UPDATE record set recordid = DEFAULT;");
            PreparedStatement statement2 = connection.prepareStatement("ALTER SEQUENCE record_recordid_seq RESTART WITH 1;");

            statement1.execute();
            statement2.execute();
            statement1.execute();

        } catch (SQLException e){
            System.err.println("Failed to reset sequences");
        }
    }

    public Set<String> getUrls() throws SQLException {
        Set<String> urlSet = new HashSet<>();

        ResultSet urls = connection.createStatement().executeQuery("select url from record;");

        while (urls.next()) {
            urlSet.add(urls.getString("url"));
        }

        return urlSet;
    }

    @Override
    protected void finalize() throws  Throwable {
        if (connection != null || !connection.isClosed()){
            connection.close();
        }
    }
}
