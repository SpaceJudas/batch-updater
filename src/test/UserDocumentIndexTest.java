package test;

import io.DbDriverImpl;
import batchmode.UserDocumentIndex;
import batchmode.data.DocumentUpdate;
import batchmode.data.User;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The UserDocumentIndex contains very little state information. This class is essentially a collection used to map
 * batch mode and the raw indexes. As such, its methods are fairly difficult to unit test. If it can be shown that its methods
 * have some degree of relative safety then its validity should truly come out in unit testing.
 */
public class UserDocumentIndexTest {
    private List<User> users;
    private DbDriverImpl db;

    @Before
    public void before() {
        this.users = Arrays.asList(new User("Han Solo", "Smuggler", "Shipping"), new User("Luke Skywalker", "Jedi", "Agriculture"),
                new User("Old Ben Kenobi", "Jedi", "Education"), new User("Darth Vader", "Jedi", "Military"),
                new User("Mr. Tarkin", "Grand Moff", "Military"), new User("Mr. Palpatine", "Emperor", "Government"),
                new User("Leia Organa", "Senator", "Government"));
        db = new DbDriverImpl();

        for(int i=0; i<users.size(); i++) {
            db.update((long) i, users.get(i).toJsonString());
        }
    }
    @Test
    public void testBuildIndexes() {
        HashSet<String> industries = new HashSet<String>();
        HashSet<String> jobTitles = new HashSet<String>();
        for(User u : users) {
            industries.add(u.getIndustry());
            jobTitles.add(u.getJobTitle());
        }
        //Test exception handling
        for(int i=0; i<users.size(); i++) {
            db.addExceptionTest(i);
        }
        UserDocumentIndex udi = new UserDocumentIndex(db);
        Set<String> udiJTS=udi.getJobTitleSet();
        Set<String> udiIS=udi.getIndustrySet();
        assertTrue(udiJTS.containsAll(jobTitles));
        assertTrue(udiIS.containsAll(industries));
        assertTrue(jobTitles.containsAll(udiJTS));
        assertTrue(industries.containsAll(udiIS));
    }

    /**
     * Change job title is a function that has little to no knowledge of its context. If we are able to show that the
     * items around it (UserFieldIndex and BatchUpdater) are relatively safe and that this is
     */
    @Test
    public void testChangeJobTitle() {
        UserDocumentIndex udi = new UserDocumentIndex(db);
        assertTrue(udi.getJobTitleSet().contains("Jedi"));
        int count0 = 0; int count1 = 0; int count2 = 0;
        for(User u : users) {
            if (u.getJobTitle().equals("Doctor"))
                count0++;
            else if (u.getJobTitle().equals("Jedi"))
                count1++;
            else if (u.getJobTitle().equals("Senator"))
                count2++;
        }
        Collection<Long> ids0 = udi.changeJobTitle("Doctor", "Something else");
        assertEquals(count0, ids0.size());
        assertTrue(count0==0);
        Collection<Long> ids1 = udi.changeJobTitle("Jedi", "Doctor");
        assertEquals(count1, ids1.size());
        assertTrue(count1>0);
        Collection<Long> ids2 = udi.changeJobTitle("Senator", "Doctor");
        assertEquals(count2, ids2.size());
        assertTrue(count2>0);
        Collection<Long> ids3 = udi.changeJobTitle("Doctor", "Lawyer");
        assertEquals(ids1.size()+ids2.size(), ids3.size());
        assertTrue(ids3.size()>0);
        Collection<Long> ids4 = udi.changeJobTitle("Doctor", "Mountain Climber");
        assertEquals(0, ids4.size());  //idempotent because running the same method on the same inputs has no new effect

        //Show that ids1+ids2 is the same set as ids3. We already know they have same size
        ids1.addAll(ids2);
        assertTrue(ids3.containsAll(ids1));
        assertTrue(ids1.containsAll(ids3));
    }
    @Test
    public void testChangeIndustry() {
        UserDocumentIndex udi = new UserDocumentIndex(db);
        assertTrue(udi.getIndustrySet().contains("Military"));
        int count0 = 0; int count1 = 0; int count2 = 0;
        for(User u : users) {
            if (u.getIndustry().equals("Food Service"))
                count0++;
            else if (u.getIndustry().equals("Military"))
                count1++;
            else if (u.getIndustry().equals("Government"))
                count2++;
        }
        Collection<Long> ids0 = udi.changeIndustry("Software Engineering", "Something else");
        assertEquals(count0, ids0.size());
        assertTrue(count0==0);
        Collection<Long> ids1 = udi.changeIndustry("Military", "Software Engineering");
        assertEquals(count1, ids1.size());
        assertTrue(count1>0);
        Collection<Long> ids2 = udi.changeIndustry("Government", "Software Engineering");
        assertEquals(count2, ids2.size());
        assertTrue(count2>0);
        Collection<Long> ids3 = udi.changeIndustry("Software Engineering", "Academia");
        assertEquals(ids1.size()+ids2.size(), ids3.size());
        assertTrue(ids3.size()>0);
        Collection<Long> ids4 = udi.changeIndustry("Software Engineering", "Crime Fighting");
        assertEquals(0, ids4.size());  //idempotent because running the same method on the same inputs has no new effect

        //Show that ids1+ids2 is the same set as ids3. We already know they have same size
        ids1.addAll(ids2);
        assertTrue(ids3.containsAll(ids1));
        assertTrue(ids1.containsAll(ids3));
    }
    @Test
    public void testUpdateId() {
        db = new DbDriverImpl();
        UserDocumentIndex udi = new UserDocumentIndex(db);
        //test insert
        assertTrue(udi.updateId(30l, new DocumentUpdate(null, "job", null, "industry", 1)));
        //Running the same call twice in a row will have no effect and should thus return false
        assertFalse(udi.updateId(30l, new DocumentUpdate(null, "job", null, "industry", 1)));
        //test job title change
        assertTrue(udi.updateId(30l, new DocumentUpdate("job", "doctor", null, null,1)));
        assertFalse(udi.updateId(30l, new DocumentUpdate("job", "doctor", null, null, 1)));
        //test industry change
        assertTrue(udi.updateId(30l, new DocumentUpdate(null, null, "industry", "medicine",1)));
        assertFalse(udi.updateId(30l, new DocumentUpdate(null, null, "industry", "medicine", 1)));
        //test delete
        assertTrue(udi.updateId(30l, new DocumentUpdate("doctor", null, "medicine", null,1)));
        assertFalse(udi.updateId(30l, new DocumentUpdate("doctor", null, "medicine", null, 1)));
    }
}
