package com.crawler;

import com.database.DB;

import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Created by grantdeshazer on 4/29/17.
 */
public class Spider {
    public static final DB db = new DB();

    private static int _recordID;
    private static int _visitedPages;
    private static int _totalLinks = 0;
    private static final int MAX_PAGES_TO_SEARCH = 10;
    private static final int DELAY_TO_REQUEST = 1000 * 5;
    private static final int NUMBER_OF_LINKS_PER_DOMAIN = 10;


    public static void main (String[] args) throws SQLException, IOException{
        _recordID = getMax("recordid");

        processPage("http://www.mines.edu");
    }


    public static void processPage(String url) throws SQLException, IOException{
        while(_visitedPages <= MAX_PAGES_TO_SEARCH) {
            String sql = "";
            String currentURL = "";
            Statement statement = null;

            SpiderLeg leg = new SpiderLeg();

            ResultSet unvisitedPages = getUnvisited();


            if (unvisitedPages.next()) {

                if(unvisitedPages.getString("url") == url){
                    currentURL = url;
                    sql = "update record set visited=true where url='" + url + "';";
                    statement = db.connection.createStatement();
                    statement.executeUpdate(sql);
//                    db.connection.commit();
                    _visitedPages++;
                } else {
                    currentURL = nextURL();
                }

            } else {
                currentURL = url;
                System.out.println("There are no unvisited sites! Going to default site");
                _visitedPages++;
            }

            leg.crawl(currentURL);

            List<String> newPageSql  = leg.getPages();
            ResultSet capturedURLS = db.connection.createStatement().executeQuery("select url from record");

            while(capturedURLS.next()) {
                newPageSql = newPageSql.stream().filter(s -> {
                    try {
                        String str = capturedURLS.getString("url");
//                        System.out.print("Checking: " + str + " against: " + s);
                        if(str.contains(s)){
//                            System.out.println(" ::matched");
                            return false;
                        } else {
//                            System.out.println(" ::didn't match");
                            return true;
                        }
                    } catch (Exception e){
                        System.err.println("Couldn't get url from visited pages");
                    }
                    return true;
                }).collect(Collectors.toList());
            }

            newPageSql = newPageSql.stream().filter(moreThanMaxDomainNames()).collect(Collectors.toList());

            _totalLinks += newPageSql.size();

            newPageSql.stream().map(s -> "insert into record ( recordid, url, visited) values ( RECORDID, '" + s + "', false);")
                    .map(s -> {
                        s = s.replaceFirst("RECORDID", Integer.toString(_recordID));
                        _recordID++;
                        return s;
                    }).forEach(s -> {
                        try {
//                            System.out.println("Doing command: " + s + " :: On database");
                            db.update(s);
                        } catch (Exception e) {
                            System.err.println("Failed to update database");
                            e.printStackTrace();
                        }
                    });

            try{
                Thread.sleep(DELAY_TO_REQUEST);
            } catch (Exception e){
                System.err.println("Failed to sleep");
                e.printStackTrace();
            }
        }

        System.out.println("Collected and stored " + _totalLinks + " web links in this crawl session");
    }


    public static String nextURL(){
        String nextURL = "";

        try {
            ResultSet unvisited = getUnvisited();

            if(unvisited.next()){
                nextURL = unvisited.getString("url");

                String sql = "update record set visited=true where url='" + nextURL + "';";
                Statement statement = db.connection.createStatement();
                statement.executeUpdate(sql);
//                db.connection.commit();
                _visitedPages++;

            } else {
                System.out.println("No unvisited Pages left!");
            }


        } catch (SQLException e){
            e.printStackTrace();
        }

        return nextURL;
    }


    private static ResultSet getUnvisited() throws SQLException {
        String sql;
        sql = "select * from record where visited=false;";
        return db.runSql(sql);
    }


    private static ResultSet getVisited() throws SQLException {
        String sql = "select * from record where visited=TRUE;";
        return db.runSql(sql);
    }


    private static int getMax(String collumn) throws SQLException{
        String sql = "select max(" + collumn + ") from record";
        ResultSet resultSet = db.runSql(sql);

        if(resultSet.next()){
            return resultSet.getInt("max");
        } else {
            return 1;
        }

    }


    private static Predicate<String> moreThanMaxDomainNames(){
        return p -> {
          try {
              URI uri = new URI(p);
              String domain = uri.getHost();
//              System.out.println("Domain name to be tested: " + domain);
              ResultSet resultSet = db.connection.createStatement()
                      .executeQuery("select count(*) from record where url like '%" + domain + "%';");

              if(resultSet.next()){
//                  int temp = resultSet.getInt("count");
                  if(resultSet.getInt("count") >= NUMBER_OF_LINKS_PER_DOMAIN) {
//                      System.out.println("More than " + NUMBER_OF_LINKS_PER_DOMAIN + " for the domain " + domain);
                      return false;
                  }
              }

          } catch (Exception e){
              e.printStackTrace();
          }

          return true;
        };
    }

}
