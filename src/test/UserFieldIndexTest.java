package test;

import batchmode.UserFieldIndex;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.*;

public class UserFieldIndexTest {
    UserFieldIndex userFieldIndex;
    Collection<Long> batch1;
    Collection<Long> batch2;

    @Before
    public void before() {
        userFieldIndex = new UserFieldIndex();
        batch1 = new HashSet<Long>();
        batch2 = new HashSet<Long>();
        for (long i = 0; i<10; i++) {
            if (i%2 == 0) batch1.add(i);
            else batch2.add(i);
        }
    }

    @Test
    public void testPutBatch() {
        userFieldIndex.putBatch("a", batch1);
        assertEquals(batch1, userFieldIndex.removeBatch("a"));
        int batch1Len = batch1.size();
        int batch2Len = batch2.size();
        userFieldIndex.putBatch("a", batch1);
        userFieldIndex.putBatch("a", batch2);
        assertEquals(batch1Len, batch1.size());
        assertEquals(batch2Len, batch2.size());
        assertEquals(batch2Len+batch1Len, userFieldIndex.removeBatch("a").size());
    }

    @Test
    public void testRemoveBatch() {
        assertNull(userFieldIndex.removeBatch("b"));
        userFieldIndex.putBatch("a", batch1);
        assertEquals(userFieldIndex.removeBatch("a").size(), batch1.size());
        assertNull(userFieldIndex.removeBatch("a"));
    }

    @Test
    public void testModifyBatchKey() {
        userFieldIndex.putBatch("a", batch1);
        userFieldIndex.putBatch("c", batch2);
        userFieldIndex.modifyBatchKey("a", "b");
        assertNull(userFieldIndex.removeBatch("a"));
        assertEquals(userFieldIndex.removeBatch("b"), batch1);
        userFieldIndex.putBatch("a", batch1);
        userFieldIndex.modifyBatchKey("a", "c");
        assertNull(userFieldIndex.removeBatch("a"));
        assertEquals(batch1.size()+batch2.size(), userFieldIndex.removeBatch("c").size());

    }
    @Test
    public void testPutId() {
        userFieldIndex.putId("a", 42l);
        Collection<Long> result1 = userFieldIndex.removeBatch("a");
        assertEquals(1, result1.size());
        assertEquals(42l, result1.iterator().next().longValue());
        userFieldIndex.putBatch("a", batch1);
        userFieldIndex.putId("a", 42l);
        Collection<Long> result2 = userFieldIndex.removeBatch("a");
        assertEquals(result2.size(), batch1.size() + 1);
        assertTrue(result2.contains(42l));
    }
    @Test
    public void testRemoveId() {
        assertFalse(userFieldIndex.removeId("a", 1l));
        userFieldIndex.putId("a", 1l);
        assertTrue(userFieldIndex.removeId("a", 1l));
        userFieldIndex.putBatch("b", batch2);
        assertTrue(userFieldIndex.removeId("b", 1l));
        assertFalse(userFieldIndex.removeBatch("b").contains(1l));
    }
    @Test
    public void testModifyIdKey() {
        userFieldIndex.putId("a", 42l);
        userFieldIndex.modifyIdKey("a", "b", 42l);
        assertFalse(userFieldIndex.containsId("a", 42l));
        assertTrue(userFieldIndex.containsId("b", 42l));
        userFieldIndex.putBatch("c", batch1);
        userFieldIndex.putId("c", 43l);
        userFieldIndex.modifyIdKey("c", "d", 43l);
        assertFalse(userFieldIndex.containsId("c", 43l));
        assertTrue(userFieldIndex.containsId("d", 43l));
    }
}
