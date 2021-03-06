package com.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by grantdeshazer on 5/2/17.
 *
 * Cleaner class:
 *
 *   This class can be run independently via its main function, or as part of the closing tasks for a crawl session.
 *
 *   Cleaner objects filter out urls containing a word or phrase in the MATURE_FILTER pattern.  The filtering can be
 *   done either for the entirety of the table "record" in the database, or can be done on individual urls.  URL's that
 *   match any of the terms in the mature filter are then deleted from the database, if they are already in it, and then
 *   added to the blacklist.
 *
 *   This class is meant to run in conjunction with a working blacklist and add to it as necessary.
 *
 *
 */
public class Cleaner {

    private static DB db;
    private int _caughtLinks = 0;

    private static final Pattern MATURE_FILTER = Pattern.compile("(sex|nsfw|gif|jpg)");

    public static void main(String[] args){
        Cleaner cleaner = new Cleaner();
        cleaner.clean();
    }

    public Cleaner(){
        db = new DB();
    }

    public Cleaner(DB dataB){
        db = dataB;
    }


    public void clean(){
        try {
            PreparedStatement statement = db.connection.prepareStatement("SELECT recordid, url FROM record");
            ResultSet urls = statement.executeQuery();
            Matcher m;

            while(urls.next()){
                String url = urls.getString("url");
                m = MATURE_FILTER.matcher(url);

                if(m.find()){
                    System.out.println("Found url with questionable content: " + url + "\nDeleting url from database " +
                            "and adding to blacklist");

                    statement = db.connection.prepareStatement("DELETE FROM record WHERE recordid=?");
                    statement.setInt(1, urls.getInt("recordid"));
                    statement.execute();

                    statement = db.connection.prepareStatement("INSERT INTO blacklist (url) VALUES (?)");
                    statement.setString(1, url);
                    statement.execute();
                    _caughtLinks++;
                }

            }

        } catch (SQLException e){
            System.err.println("Problem with sql");
            e.printStackTrace();
        }

        try {
            PreparedStatement statement = db.connection.prepareStatement("SELECT count(url) FROM record;");
            ResultSet URLcount = statement.executeQuery();

            statement = db.connection.prepareStatement("SELECT max(recordid) FROM record;");
            ResultSet maxRecordID = statement.executeQuery();

            if(URLcount.next() && maxRecordID.next()){
                if(URLcount.getInt("count") == maxRecordID.getInt("max")){
                    System.out.println("Found difference between sequence max and number of rows.  Resetting sequence.");
                    db.resetSequence();
                }
            }

        } catch (SQLException e){
            System.err.println("Couldn't reset sequence");
            e.printStackTrace();
        }

        System.out.println("Found, deleted, and blacklisted " + _caughtLinks + " links.");
    }


    public boolean checkIfClean(String url){
      Matcher m = MATURE_FILTER.matcher(url);

      if(m.find()){
          db.deleteFromDB(url);
          db.addToBlacklist(url);
          return false;
      }

      return true;
    }


    public Predicate<String> checkIfCleanPredicate(){
        return p -> {
            Matcher m = MATURE_FILTER.matcher(p);
            if(m.find()){
                db.deleteFromDB(p);
                db.addToBlacklist(p);
                return false;
            } else {
                return true;
            }
        };
    }

}
