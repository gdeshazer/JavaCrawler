package com.crawler;

import com.database.Cleaner;
import com.database.DB;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Created by grantdeshazer on 4/29/17.
 *
 * TODO: Add Junit tests
 * TODO: Implement asynchronous HTTP calls
 * TODO: Add a logfile generator to collect visited sites and running parameters from a crawl
 * TODO: Add front end interface
 * TODO: Add robot.txt compliance
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
 *      - FILTER1                      ::  patterns to filter from the database, prevents pages which link to pdf's
 *                                         and similar from being used in the crawler
 *      - FILTER2                      ::  patterns to check for in url, used to verify url is mostly valid
 *
 *    Assumptions:
 *      - The database table has already been created as follows, it will not check if it does exist:
 *          create table record (
 *              recordid serial primary key,
 *              url text unique,
 *              visited boolean,
 *          );
 *
 *          create table blacklist (
 *              id serial primary key,
 *              url text
 *          );
 *
 *      - database connection will be handled and closed by another class
 *      - assumes database is set to auto commit
 *      - if all weblinks are marked as visited, or database is empty, the crawler will go to the page passed to
 *        processPage
 *      - assumes the blacklist already has some urls in it
 *          - initially filled with websites from Shalla list http://www.shallalist.de/
 *
 */
public class Spider {
    public final DB db;
    public Cleaner _cleaner;

    private int _visitedPages;
    private int _totalLinks = 0;

    private final int MAX_PAGES_TO_SEARCH = 15;
    private final int DELAY_TO_REQUEST = 1000;
    private final int NUMBER_OF_LINKS_PER_DOMAIN = 5;

    private final Pattern FILTER1 = Pattern.compile(".*(\\.(css|gif|jpg|js|png|mp3|mp4|zip|rss_1|pdf))$");
    private final Pattern FILTER2 = Pattern.compile("^http[s]*");

    private List<String> _streamCheck;


    public static void main (String[] args) throws SQLException, IOException{
        Spider spider = new Spider();

        spider.processPage("http://www.mines.edu");

        spider._cleaner.clean();
    }


    public Spider(){
        db = new DB();
        _cleaner = new Cleaner(db);
    }

    public Spider(String database){
        db = new DB(database);
        _cleaner = new Cleaner(db);
    }


    public void processPage(String url) throws SQLException, IOException {

        String currentURL = "";

        SpiderLeg leg = new SpiderLeg();

        currentURL = getUnvisitedStartingURL(url);

        while (_visitedPages <= MAX_PAGES_TO_SEARCH) {

            leg.crawl(currentURL);

            List<String> newUrls = leg.getNewUrls();

            Set<String> capturedURL = db.getUrls();

            newUrls = filterUrls(currentURL, newUrls, capturedURL);

            db.commitUrlsToDB(newUrls);

            try {
                Thread.sleep(DELAY_TO_REQUEST);
            } catch (Exception e) {
                System.err.println("Failed to sleep");
                e.printStackTrace();
            }

            currentURL = nextURL();
        }

        System.out.println("Collected and stored " + _totalLinks + " web links in this crawl session.  Cleaning up DB...");
    }


    public List<String> filterUrls(String currentURL, List<String> newPageSql, Set<String> capturedURLS) {
        // need to have !capturedURL otherwise the filter gets rid of URLS which do not exist in the DB
        List<String> newPages = newPageSql.stream().filter(url -> !capturedURLS.contains(url)).collect(Collectors.toList());

        _streamCheck = newPages;

        newPages = newPages.stream()
                .filter(moreThanMaxDomainNamesStream())
                .filter(moreThanMaxDomainNamesInDB())
                .filter(isURLHTTPPredicate().and(doesNotContainNonHTMLTypePredicate()))
                .filter(_cleaner.checkIfCleanPredicate())
                .collect(Collectors.toList());

        _totalLinks += newPages.size();

        System.out.println("From url: " + currentURL + " :: Committing " + newPages.size() + " links to db");

        return newPages;
    }


    private String getUnvisitedStartingURL(String url) throws SQLException {
        PreparedStatement statement;
        String currentURL;

        statement = db.connection.prepareStatement("SELECT url FROM record WHERE url=?;");
        statement.setString(1,url);

        ResultSet urlAlreadyInDB = statement.executeQuery();

        //if url is already in database
        //  if visited get new url
        //  else if not, mark as visited and return it as url to crawl on
        //else if url is not in the database and database is empty
        //  add url to database, mark as visited, and return url to crawl on
        //else get new url
        if(urlAlreadyInDB.next() ){
            if (urlAlreadyInDB.getString("url").contains(url)){
                currentURL = nextURL();
            } else {
                currentURL = url;
                statement = db.connection.prepareStatement("UPDATE record SET visited=TRUE WHERE url=?;");
                statement.setString(1, url);
                statement.execute();
                _visitedPages++;
            }
        } else if (!db.connection.prepareStatement("select * from record;").executeQuery().next()) {
            currentURL = url;
            statement = db.connection.prepareStatement("insert into record (url,visited) values (?, true);");
            statement.setString(1, url);
            statement.execute();
            _visitedPages++;
        } else {
            currentURL = nextURL();
        }
        return currentURL;
    }


    private String nextURL(){
        String nextURL = "";

        PreparedStatement statement;
        ResultSet count;
        ResultSet resultSet;

        int randomInt = 0;

        boolean newURL = false;

        try {
            statement = db.connection.prepareStatement("SELECT count(recordid) FROM record;");
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
                    if (!resultSet.getBoolean("visited") && !onBlacklist(url) && !containsNonHTMLType(url)
                            && isURLHTTP(url)) {
                        nextURL = url;
                        statement = db.connection.prepareStatement("UPDATE record SET visited=TRUE WHERE url=?;");
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


    private boolean onBlacklist(String url){
        try{
            URI uri = new URI(url);
            String domain = uri.getHost();
            PreparedStatement statement = db.connection.prepareStatement("SELECT url FROM blacklist WHERE url=? OR url=? OR url LIKE ?;");
            statement.setString(1, url);
            statement.setString(2, domain);
            statement.setString(3, "%" + domain + "%");

            ResultSet resultSet = statement.executeQuery();

            if(resultSet.next()){
                System.out.println("Blacklisted :: " + url);
                return true;
            }


        } catch (URISyntaxException e ){
            System.err.println("Error in URI string.  Can't determine if URL is in blacklist.  Ignoring URL");
            return true;
        } catch (SQLException e){
            System.err.println("Couldn't query database");
            System.err.println(e.getMessage() + "\nCause:: " + e.getCause());
            return true;
        }

        return false;
    }


    private boolean containsNonHTMLType(String url){
        Matcher matcher = FILTER1.matcher(url);
        if (matcher.find()) {
            return true;
        }
        return false;
    }


    private boolean isURLHTTP(String url){
            Matcher matcher = FILTER2.matcher(url);
            if(matcher.find()){
                return true;
            } else {
                return false;
            }
    }



    private Predicate<String> doesNotContainNonHTMLTypePredicate(){
        return p -> {
            Matcher matcher = FILTER1.matcher(p);
            if (matcher.find()) {
                return false;
            } else {
                return true;
            }
        };
    }


    private Predicate<String> isURLHTTPPredicate(){
        return p -> {
            Matcher matcher = FILTER2.matcher(p);
            if(matcher.find()){
                return true;
            } else {
                return false;
            }
        };
    }


    private Predicate<String> moreThanMaxDomainNamesInDB(){
        return p -> {
            try {
                URI uri = new URI(p);
                String domain = uri.getHost();
                PreparedStatement statement = db.connection.prepareStatement("SELECT count(url) FROM record WHERE url LIKE ? OR url=?;");
                statement.setString(1, "%"+ domain + "%");
                statement.setString(2, p);
                ResultSet resultSet = statement.executeQuery();

                if(resultSet.next()){
                    if(resultSet.getInt("count") >= NUMBER_OF_LINKS_PER_DOMAIN) {
                        return false;
                    } else {
                        return true;
                    }
                }

            } catch (URISyntaxException e){
                return false;
            } catch (SQLException e){
                return false;
            }
            return true;
        };
    }


    private Predicate<String> moreThanMaxDomainNamesStream(){
        return p -> {
            int count = 0;

            try {
                URI uri = new URI(p);
                String domain = uri.getHost();

                Pattern pattern = Pattern.compile(".*" + domain + ".*");

                for(String s : _streamCheck) {
                    Matcher matcher = pattern.matcher(s);

                    if(matcher.find()){
                        count++;
                    }
                }

                if(count >= NUMBER_OF_LINKS_PER_DOMAIN){
                    return false;
                } else {
                    return true;
                }

            } catch (URISyntaxException e){
                System.err.println("Error in url passed to URI :: " + e.getReason() + "\nURL ::\" " + p + "\"\n");
                return false;
            }
        };
    }

}
