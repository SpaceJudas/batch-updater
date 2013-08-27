package test;

import batchmode.data.DocumentUpdate;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DocumentUpdateTest {
    @Test
    public void testAll() {
        testCompareTo();
        testIsJobTitleChange();
        testIsIndustryChange();
        testIsInsert();
        testIsDelete();
    }
    @Test
    public void testCompareTo() {
        DocumentUpdate dc1= new DocumentUpdate(null, null, null, null, 5l);
        DocumentUpdate dc2 = new DocumentUpdate(null, null, null, null, 1l);
        assertEquals(4, dc1.compareTo(dc2));
        assertEquals(-4, dc2.compareTo(dc1));
        assertEquals(0, dc1.compareTo(dc1));
    }

    @Test
    public void testIsJobTitleChange() {
        HashMap<String, Object> changeMap = new HashMap<String, Object>();
        changeMap.put("old_job_title", "Moisture Farmer");
        changeMap.put("new_job_title", "Jedi");
        DocumentUpdate changeObj = new DocumentUpdate(changeMap);
        assertTrue(changeObj.isJobTitleChange());
        assertFalse(changeObj.isIndustryChange());
        assertFalse(changeObj.isInsert());
        assertFalse(changeObj.isDelete());
    }

    @Test
    public void testIsIndustryChange() {
        HashMap<String, Object> changeMap = new HashMap<String, Object>();
        changeMap.put("old_industry", "Agriculture");
        changeMap.put("new_industry", "Rebellion");
        DocumentUpdate changeObj = new DocumentUpdate(changeMap);
        assertFalse(changeObj.isJobTitleChange());
        assertTrue(changeObj.isIndustryChange());
        assertFalse(changeObj.isInsert());
        assertFalse(changeObj.isDelete());
    }

    @Test
    public void testIsInsert() {
        HashMap<String, Object> changeMap = new HashMap<String, Object>();
        changeMap.put("new_job_title", "Jedi");
        changeMap.put("new_industry", "Rebellion");
        DocumentUpdate changeObj = new DocumentUpdate(changeMap);
        assertFalse(changeObj.isJobTitleChange());
        assertFalse(changeObj.isIndustryChange());
        assertTrue(changeObj.isInsert());
        assertFalse(changeObj.isDelete());
    }

    @Test
    public void testIsDelete() {
        HashMap<String, Object> changeMap = new HashMap<String, Object>();
        changeMap.put("old_job_title", "Moisture Farmer");
        changeMap.put("old_industry", "Agriculture");
        DocumentUpdate changeObj = new DocumentUpdate(changeMap);
        assertFalse(changeObj.isJobTitleChange());
        assertFalse(changeObj.isIndustryChange());
        assertFalse(changeObj.isInsert());
        assertTrue(changeObj.isDelete());
    }
}
