package test;

import batchmode.data.User;
import org.json.simple.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

public class UserTest {
    @Test
    public void testAll() {
        testConstructor_NameTitleIndustry();
        testConstructor_NameTitleIndustryVersion();
        testConstructor_JsonString();
        testEqualsAndHashcode();
    }
    @Test
    public void testConstructor_NameTitleIndustry() {
        User u = new User("Luke", "Jedi", "Agriculture");
        assertEquals(u.getName(), "Luke");
        assertEquals(u.getJobTitle(), "Jedi");
        assertEquals(u.getIndustry(), "Agriculture");
        assertEquals(u.getVersion(), 1l);
    }
    @Test
    public void testConstructor_NameTitleIndustryVersion() {
        User u = new User("Luke", "Jedi", "Agriculture", 3l);
        assertEquals(u.getName(), "Luke");
        assertEquals(u.getJobTitle(), "Jedi");
        assertEquals(u.getIndustry(), "Agriculture");
        assertEquals(u.getVersion(), 3l);
    }
    @Test
    public void testConstructor_JsonString() {
        JSONObject jo = new JSONObject();
        jo.put("name", "Luke");
        jo.put("job_title", "Jedi");
        jo.put("industry", "Agriculture");
        //test construction from json without version number
        jo.remove("version");
        User u = new User(jo.toString());
        assertEquals(u.getName(), "Luke");
        assertEquals(u.getJobTitle(), "Jedi");
        assertEquals(u.getIndustry(), "Agriculture");
        assertEquals(u.getVersion(), 1l);
        //test construction from json with version number
        jo.put("version", 3);
        u = new User(jo.toString());
        assertEquals(u.getName(), "Luke");
        assertEquals(u.getJobTitle(), "Jedi");
        assertEquals(u.getIndustry(), "Agriculture");
        assertEquals(u.getVersion(), 3l);
    }
    @Test
    public void testEqualsAndHashcode() {
        User u1 = new User("Luke", "Jedi", "Agriculture");
        User u2 = new User("Luke", "Jedi", "Agriculture", 1l);
        User u3 = new User("Luke", "Jedi", "Agriculture");
        assertTrue(u1.equals(u2));                                   //reflexive
        assertTrue(u1.equals(u2) && u2.equals(u3) && u1.equals(u3)); //transitive
        assertTrue(u1.equals(u2) && u2.equals(u1));                  //symmetric
        assertEquals(u1.hashCode(), u2.hashCode());
        assertEquals(u1.hashCode(), u3.hashCode());
        //test for not equal
        u2.setJobTitle("");
        assertFalse(u1.equals(u2));
        assertNotSame(u1.hashCode(), u2.hashCode());
        u3.setIndustry("");
        assertFalse(u1.equals(u3));
        assertNotSame(u1.hashCode(), u2.hashCode());
        //there exist many more untested falsity cases
    }
}
