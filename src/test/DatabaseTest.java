package test;

import io.DbDriverImpl;
import batchmode.data.User;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DatabaseTest {
    private DbDriverImpl db;
    @Before public void beforeTest() {
        db = new DbDriverImpl();
        for(long i = 0; i<10; i++) {
            db.update(i, new User("name" + i, "job" + i, "industry" + i, 1l).toString());
        }

        //The star was universe circa before-new hope
        new User("Han Solo", "Smuggler", "Shipping");
        new User("Luke Skywalker", "Jedi", "Agriculture");
        new User("Old Ben Kenobi", "Jedi", "Education");
        new User("Darth Vader", "Jedi", "Military");
        new User("Mr. Tarkin", "Grand Moff", "Military");
        new User("Mr. Palpatine", "Emperor", "Government");
        new User("Leia Organa", "Senator", "Government");

    }

    @Test
    public void testRead() {
        for(long i = 0; i<10; i++) {
            assertEquals(db.read(i), new User("name"+i, "job"+i, "industry"+i, 1l).toString());
        }
    }
    @Test
    public void testException() {
        Iterator it = db.scan();
        db.addExceptionTest(5);
        for(int i = 5; i>=0; i--) {
            try {
                it.next();
            }
            catch (Exception e) {
                assertEquals(5, i);
                return;
            }
        }
        assertFalse(true);
    }
}
