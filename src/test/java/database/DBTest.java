package database;

import org.junit.*;
import com.database.DB;
import org.junit.Test;

import static junit.framework.TestCase.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



/**
 * Created by grantdeshazer on 5/27/17.
 */
public class DBTest {

    private DB db = new DB("crawlTest");

    @Test
    public void queryTestPass(){
        String sql = "Select * from record;";

        try{
            ResultSet set = db.queryDB(sql);

            Assert.assertTrue(set.next());

        } catch (SQLException e){
            fail("Recieved SQL exception with message :: " + e.getCause());
        }
    }

    @Test
    public void queryTestThrowsException(){
        String sql = "Select * from recrd;"; //incorrect table name -> Should fail

        try {
            ResultSet set = db.queryDB(sql);
            fail("Didn't produce an exception");
        } catch (Exception e ){
            //expected
        }

    }

    @Test
    public void executeStatementTest(){
        try {
            Assert.assertFalse(db.executeStatement("insert into record (url, visited) values ('insertTest.com', true);"));
            db.deleteFromDB("insertTest.com");
        } catch (Exception e){
            fail("Produced unexpected exception while executing an insert");
        }
    }

    @Test public void exectuteStatementThrowsException(){
        try{
            Assert.assertFalse(db.executeStatement("insert into recd (url, visited) values ('thisShouldFail', false);"));
            fail("Didn't throw exception");
        } catch (Exception e) {
            //expected
        }
    }

    @Test
    public void deleteFromDBTest() {
        String url = "http://urlToDelete.com";
        try {

            db.executeStatement("Insert into record (url, visited) values ('" + url + "',false);");

            db.deleteFromDB(url);

            ResultSet set = db.queryDB("select url from record where url='" + url + "';");

            Assert.assertFalse(set.next());

        } catch(Exception e){
            fail("Something went wrong when deleting url form database");
        }
    }

    @Test
    public void addToBlacklistTest(){
        String url = "http://blacklistTest.com";

        db.addToBlacklist(url);

        try{
            ResultSet set = db.queryDB("select url from blacklist where url like '%blacklistTest.com';");

            Assert.assertTrue(set.next());

            db.update("delete from blacklist where url like '%blacklistTest.com';");

        } catch (Exception e) {
            fail("Something went wrong in addToBlacklistTest :: " + e.getCause());
        }
    }

    @Test
    public void resetSequenceTest(){
        db.resetSequence();

        try {
            PreparedStatement statement = db.connection.prepareStatement("SELECT count(url) FROM record;");
            ResultSet URLcount = statement.executeQuery();

            statement = db.connection.prepareStatement("SELECT max(recordid) FROM record;");
            ResultSet maxRecordID = statement.executeQuery();

            if(URLcount.next() && maxRecordID.next()){
                if(URLcount.getInt("count") != maxRecordID.getInt("max")){
                    fail("Sequence failed to reset");
                }
            }

        } catch (SQLException e){
            fail("failed to reset sequence.  Got sql exception");
        }

    }

    @Test
    public void getUrlsTest(){
        //TODO: Test for returned URL Accuracy
    }


    @Test
    public void commitUrlsToDBTest(){
        //TODO: Test for adding urls to DB
    }

}
