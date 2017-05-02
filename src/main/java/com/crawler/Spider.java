package com.crawler;

import com.database.DB;

import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Created by grantdeshazer on 4/29/17.
 *
 * TODO: Add robot.txt compliance
 * TODO: Add front end interface
 * TODO: Add a logfile generator to collect visited sites and running parameters from a crawl
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
 *              recordid sequence primary key,
 *              url text unique,
 *              visited boolean,
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
    private static final int DELAY_TO_REQUEST = 500;
    private static final int NUMBER_OF_LINKS_PER_DOMAIN = 1;
    private static final Pattern FILTER1 = Pattern.compile(".*(\\.(css|gif|jpg|js|png|mp3|mp4|zip|rss_1|pdf))$");
    private static final Pattern FILTER2 = Pattern.compile("^http[s]*");


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

            newPageSql = newPageSql.stream().filter(moreThanMaxDomainNames())
                    .filter(isURLHTTP().and(doesNotContainNonHTMLTypePredicate()))
                    .collect(Collectors.toList());

            _totalLinks += newPageSql.size();
            System.out.println("comitting " + newPageSql.size() + " links to db");

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

        PreparedStatement statement;
        ResultSet count;
        ResultSet resultSet;

        int randomInt = 0;

        boolean newURL = false;

        try {
            statement = db.connection.prepareStatement("SELECT count(recordid) FROM record");
            count = statement.executeQuery();

            if (count.next()) {
                randomInt = count.getInt("count");
            } else {
                randomInt = (int) (10.00 * Math.random());
            }

            while (!newURL) {
                randomInt = (int) ((double) randomInt * Math.random());

                statement = db.connection.prepareStatement("SELECT url,visited FROM record WHERE recordid=?;");
                statement.setInt(1, randomInt);
                resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    String url = resultSet.getString("url");
                    if (!resultSet.getBoolean("visited") && !onBlacklist(url) && !containsNonHTMLType(url)) {
                        nextURL = url;
                        statement = db.connection.prepareStatement("UPDATE record SET visited=TRUE WHERE url=?");
                        statement.setString(1, nextURL);
                        statement.execute();
                        _visitedPages++;
                        newURL = true;
                    }
                } else {
                    System.out.println("Couldn't find a new URL");
                    break;
                }

            }

        } catch (Exception e){
            e.printStackTrace();
            System.err.println("Failed to get new webpage in method nextURL");
        }

        return nextURL;
    }


    private static boolean onBlacklist(String url){
        try{
            URI uri = new URI(url);
            String domain = uri.getHost();
            PreparedStatement statement = db.connection.prepareStatement("select url from blacklist where url=? or url=? or url like ?");
            statement.setString(1, url);
            statement.setString(2, domain);
            statement.setString(3, "%" + domain + "%");

            ResultSet resultSet = statement.executeQuery();

            if(resultSet.next()){
                System.out.println(url + "::Blacklisted");
                return true;
            }


        } catch (Exception e ){
            System.err.println("Couldn't decide if url is on blacklist.  " +
                    "Could be either a url format problem or was unable to query" +
                    " database.");
            e.printStackTrace();
            return true;
        }

        return false;
    }


    private static boolean containsNonHTMLType(String url){
        Matcher matcher = FILTER1.matcher(url);
        if (matcher.find()) {
//            System.out.println(url + " contains non-HTML type");
            return true;
        }
        return false;
    }


    private static Predicate<String> doesNotContainNonHTMLTypePredicate(){
        return p -> {
            Matcher matcher = FILTER1.matcher(p);
            if (matcher.find()) {
//                System.out.println(p + " contains non-HTML type");
                return false;
            } else {
                return true;
            }
        };
    }


    private static Predicate<String> isURLHTTP(){
        return p -> {
            Matcher matcher = FILTER2.matcher(p);
            if(matcher.find()){
//                System.out.println("Url has http start");
                return true;
            } else {
                return false;
            }
        };
    }

    private static ResultSet getUnvisited() throws SQLException {
        return db.queryDB("select * from record where visited=false;");
    }


    private static ResultSet getVisited() throws SQLException {
        return db.queryDB("select * from record where visited=TRUE;");
    }


    private static int getMax(String collumn) throws SQLException{
        String sql = "select max(" + collumn + ") from record";
        ResultSet resultSet = db.queryDB(sql);

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
                PreparedStatement statement = db.connection.prepareStatement("select count(url) from record where url like ? or url=?;");
                statement.setString(1, "%"+ domain + "%");
                statement.setString(2, p);
//                System.out.println("Executing query: " + statement.toString());
                ResultSet resultSet = statement.executeQuery();

                if(resultSet.next()){
//                  int temp = resultSet.getInt("count");
                    if(resultSet.getInt("count") >= NUMBER_OF_LINKS_PER_DOMAIN) {
//                      System.out.println("More than " + NUMBER_OF_LINKS_PER_DOMAIN + " for the domain " + domain);
//                        System.out.println("return false for " + p);
                        return false;
                    } else {
                        return true;
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
