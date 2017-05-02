package com.crawler;

import com.database.DB;

import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Created by grantdeshazer on 4/29/17.
 *
 * TODO: Add blacklist ~> a database table?
 * TODO: Add robot.txt compliance
 * TODO: Add front end interface
 * TODO: Update Database to set recordid column as a sequence
 * TODO: Add a logfile generator to collect visited sites and running parameters
 *
 * Basic Crawler/spider
 *    Crawler stores all of its links both visited and unvisited into a postgres database
 *
 *    This class does not make any attempts to close connections to the database and assumes all connection
 *    handling is dealt with in the DB class.
 *
 *    This class doesn't do any of the crawling, instead it focuses on parsing returned urls, and placing them
 *    into the database.  This class also tracks if a site has been visited or not.
 *
 *    Behavior of the crawler is controlled by the following:
 *      - MAX_PAGES_TO_SEARCH          ::  maximum number of pages to visit in a crawl session
 *      - DELAY_TO_REQUEST             ::  number of milliseconds to wait between sending another http request to server
 *      - NUMBER_OF_LINKS_PER_DOMAIN   ::  controls the number of links under a specific domain to be stored into the
 *                                         database
 *
 *    Assumptions:
 *      - The database table has already been created as follows, it will not check if it does exist:
 *          create table record (
 *              recordid int,
 *              url text,
 *              visited boolean,
 *              primary ket (recordid)
 *          )
 *      - database connection will be handled and closed by another class
 *      - assumes database is set to auto commit
 *      - if all weblinks are marked as visited, or database is empty, the crawler will go to the page passed to
 *        processPage
 *
 */
public class Spider {
    public static final DB db = new DB();

    private static int _visitedPages;
    private static int _totalLinks = 0;
    private static final int MAX_PAGES_TO_SEARCH = 10;
    private static final int DELAY_TO_REQUEST = 1000 * 5;
    private static final int NUMBER_OF_LINKS_PER_DOMAIN = 10;


    public static void main (String[] args) throws SQLException, IOException{
        processPage("http://www.mines.edu");
//        processPage("http://amazon.com/");
    }


    public static void processPage(String url) throws SQLException, IOException{
        String sql = "";
        String currentURL = "";
        PreparedStatement statement;
        boolean newStart = false;

        SpiderLeg leg = new SpiderLeg();

        statement = db.connection.prepareStatement("select url from record where url=?");
        statement.setString(1,url);
        ResultSet urlAlreadyInDB = statement.executeQuery();


        if(urlAlreadyInDB.next() ){
            if (urlAlreadyInDB.getString("url").contains(url)){
                currentURL = nextURL();
            } else {
                currentURL = url;
                statement = db.connection.prepareStatement("insert into record (url,visited) values (?, true)");
                statement.setString(1, url);
                statement.execute();
                _visitedPages++;
            }
        } else if (!db.connection.prepareStatement("select * from record").executeQuery().next()) {
            currentURL = url;
            statement = db.connection.prepareStatement("insert into record (url,visited) values (?, true)");
            statement.setString(1, url);
            statement.execute();
            _visitedPages++;
        } else {
            currentURL = nextURL();
        }


        while(_visitedPages <= MAX_PAGES_TO_SEARCH) {

            leg.crawl(currentURL);

            List<String> newPageSql  = leg.getPages();
            ResultSet capturedURLS = db.connection.createStatement().executeQuery("select url from record");

            while(capturedURLS.next()) {
                newPageSql = newPageSql.stream().filter(s -> {
                    try {
                        String str = capturedURLS.getString("url");
                        if(str.contains(s)){
                            return false;
                        } else {
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

            newPageSql.stream().forEach(s -> {
                try {
                    PreparedStatement statement1 = db.connection.prepareStatement("insert into record (url, visited) values (?, false);");
                    statement1.setString(1, s);
                    statement1.execute();
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

            currentURL = nextURL();
        }

        System.out.println("Collected and stored " + _totalLinks + " web links in this crawl session");
    }


    public static String nextURL(){
        String nextURL = "";

        try {
            ResultSet unvisited = getUnvisited();

            if(unvisited.next()){
                nextURL = unvisited.getString("url");
                String sql = "update record set visited=true where url=?;";
                PreparedStatement statement = db.connection.prepareStatement(sql);
                statement.setString(1, nextURL);
                statement.execute();
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
        return db.runSql("select * from record where visited=false;");
    }


    private static ResultSet getVisited() throws SQLException {
        return db.runSql("select * from record where visited=TRUE;");
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
                PreparedStatement statement = db.connection.prepareStatement("select count(*) from record where url like ?;");
                statement.setString(1, "'%"+ domain + "%'");
                ResultSet resultSet = statement.executeQuery();

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

    private static String formatURL(String url){
        return "'" + url + "'";
    }

}
