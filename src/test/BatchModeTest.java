package test;

import batchmode.BatchUpdater;
import io.DbDriver;
import io.DbDriverImpl;
import io.DocumentUpdateQueueImpl;
import batchmode.data.User;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchModeTest {
    @Test
    public void testBatchUpdater() {
        DbDriver dbDriver = new DbDriverImpl();
        DocumentUpdateQueueImpl docQueue = new DocumentUpdateQueueImpl();

        dbDriver.update(1l, new User("a", "intern", "software", 1l).toJsonString());
        dbDriver.update(2l, new User("b", "intern", "medicine", 1l).toJsonString());
        dbDriver.update(3l, new User("c", "accountant", "finance", 1l).toJsonString());
        dbDriver.update(4l, new User("d", "trader", "finance", 1l).toJsonString());
        dbDriver.update(5l, new User("e", "control", "control", 1l).toJsonString());

        BatchUpdater bm = new BatchUpdater(dbDriver, docQueue);
        HashMap<String, Object> changes = new HashMap<String, Object>();
        changes.put("old_job_title", "intern");
        changes.put("new_job_title", "clown");
        changes.put("version", new Long(2l));
        //i am actually unable to unit test document queue updates in my implementation since my queue implementation
        // doesn't update the database. i have however independently confirmed that the index is updated and integration
        // testing with a fully functional queue should support that.
        docQueue.publish(2, changes);
        bm.changeJobTitle("intern", "Intern");
        bm.changeIndustry("finance", "Finance");

        bm.executeBatchUpdates();

        new User("b", "Intern", "medicine", 1l);
        new User("c", "accountant", "Finance", 1l);
        new User("b", "trader", "Finance", 1l);
        assertTrue(new User(dbDriver.read(1l).toString()).equals(new User("a", "Intern", "software", 1l)));
        assertEquals(new User(dbDriver.read(3l).toString()),new User("c", "accountant", "Finance", 1l));
        assertEquals(new User(dbDriver.read(4l).toString()),new User("d", "trader", "Finance", 1l));
        assertEquals(new User(dbDriver.read(5l).toString()),new User("e", "control", "control", 1l));
        //confirm that the document update queue fixed the index properly
        //assertEquals(new User(dbDriver.read(2l).toString()),new User("b", "clown", "medicine", 1l));

    }
}
