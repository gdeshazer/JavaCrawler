package database;

import com.database.DB;
import com.database.Cleaner;

import org.junit.*;
import org.junit.Test;
import static junit.framework.TestCase.fail;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by grantdeshazer on 5/27/17.
 */
public class CleanerTest {
    private DB db = new DB("crawlTest");
    private Cleaner cleaner = new Cleaner(db);

    @Test
    public void cleanTest(){
        try {
            cleaner.clean();

            db.executeStatement("insert into record (url, visited) values ('http://example.sex.com',false);");
            db.executeStatement("insert into record (url, visited) values ('http://example.sex1.com',false);");
            db.executeStatement("insert into record (url, visited) values ('http://example.sex2.com',false);");

            cleaner.clean();

            ResultSet set  = db.queryDB("Select url from record where url='http://example.sex.com';");

            Assert.assertFalse(set.next());

            db.update("delete from blacklist where url='http://example.sex.com';");
            db.update("delete from blacklist where url='http://example.sex1.com';");
            db.update("delete from blacklist where url='http://example.sex2.com';");

            resetBlacklistSequence();


        } catch (SQLException e){
            fail("Failed to clean properly with exception : " + e.getClass() + " and cause " + e.getCause());
        }
    }

    @Test
    public void checkIfCleanTest(){
        String cleanURL = "http://www.example.com";
        String dirtyURL = "http://www.example.sex.com";
        String improperCleanURL = "http://www.example.com/? NO SPACES ALLOWED";
        String improperDirtyURL = "http://www.example.sex.com/? NO SPACES ALLOWED";

        Assert.assertTrue(cleaner.checkIfClean(cleanURL));
        Assert.assertFalse(cleaner.checkIfClean(dirtyURL));

        Assert.assertTrue(cleaner.checkIfClean(improperCleanURL));
        Assert.assertFalse(cleaner.checkIfClean(improperDirtyURL));

        resetBlacklistSequence();
    }

    @Test
    public void checkifCleanPredicateTest(){
        String cleanURL = "http://www.example.com";
        String dirtyURL = "http://www.example.sex.com";
        String improperCleanURL = "http://www.example.com/? NO SPACES ALLOWED";
        String improperDirtyURL = "http://www.example.sex.com/? NO SPACES ALLOWED";

        Assert.assertTrue(cleaner.checkIfCleanPredicate().test(cleanURL));
        Assert.assertTrue(cleaner.checkIfCleanPredicate().test(improperCleanURL));

        Assert.assertFalse(cleaner.checkIfCleanPredicate().test(dirtyURL));
        Assert.assertFalse(cleaner.checkIfCleanPredicate().test(improperDirtyURL));

        resetBlacklistSequence();
    }

    private void resetBlacklistSequence(){
        try {
            db.update("delete from blacklist where url like '%example.sex.com%';");

            db.update("Update blacklist set id = DEFAULT;");
            db.update("alter sequence blacklist_id_seq restart with 1;");
            db.update("Update blacklist set id = DEFAULT;");
        } catch (Exception e ){

        }
    }
}
