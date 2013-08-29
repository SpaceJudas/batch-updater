package batchmode;

import batchmode.UserFieldIndex;
import batchmode.data.DocumentUpdate;
import batchmode.data.User;
import io.DbDriver;

import java.util.*;

/**
 * The user document index
 */
public class UserDocumentIndex {
    final UserFieldIndex jobTitleIndex;
    final UserFieldIndex industryIndex;
    final DbDriver dbDriver;

    public UserDocumentIndex(DbDriver dbDriver) {
        this.dbDriver = dbDriver;
        this.jobTitleIndex = new UserFieldIndex();
        this.industryIndex = new UserFieldIndex();
        rebuildIndex();
    }

    /**
     * Rebuilds the index from the database given at instantiation.
     */
    public void rebuildIndex() {
        jobTitleIndex.clear();
        industryIndex.clear();
        rebuildIndex(0);
    }
    /**
     * A recursive helper function that builds the index using the DbDriver.scan(). Both timeout and concurrency
     * exceptions may occur when querying the database. Since Java has no way to copying iterators, the step count is
     * incremented with every successful record retrieval so that when an exception occurs, it gets a new iterator using
     * its step count.
     */
    private void rebuildIndex(int stepCount) {
        Iterator<Map.Entry<Long, Object>> it = dbDriver.scan(stepCount);
        Map.Entry<Long, Object> entry;
        while(it.hasNext()) {
            try {
                entry = it.next();
                User user = new User(entry.getValue().toString());
                jobTitleIndex.putId(user.getJobTitle(), entry.getKey());
                industryIndex.putId(user.getIndustry(), entry.getKey());
                stepCount++;
            }
            catch (Exception c) {
                //Retry
                rebuildIndex(stepCount);
            }
        }
    }

    public Collection<Long> changeJobTitle(String oldJobTitle, String newJobTitle) {
        return jobTitleIndex.modifyBatchKey(oldJobTitle, newJobTitle);
    }
    public Collection<Long> changeIndustry(String oldIndustry, String newIndustry) {
        return industryIndex.modifyBatchKey(oldIndustry, newIndustry);
    }
    public Set<String> getJobTitleSet() {
        return jobTitleIndex.keySet();
    }
    public Set<String> getIndustrySet() {
        return industryIndex.keySet();
    }

    /**
     * Attempts to update the user's entry in the index with the given changes. Returns <code>true</code> if the
     * attempted modification was successful and <code>false</code> if the index was not modified.
     * @param userId
     * @param changes a document update object as specified by the document update queue
     * @return
     */
    public boolean updateId(long userId, DocumentUpdate changes) {
        boolean r = false;
        if (changes.isJobTitleChange()) {
            r = jobTitleIndex.modifyIdKey(changes.getOldJobTitle(), changes.getNewJobTitle(), userId);
        }
        if (changes.isIndustryChange()) {
            r = industryIndex.modifyIdKey(changes.getOldIndustry(), changes.getNewIndustry(), userId);
        }
        if (changes.isDelete()) {
            if (jobTitleIndex.containsId(changes.getOldJobTitle(), userId)
                    && industryIndex.containsId(changes.getOldIndustry(), userId)) {
                jobTitleIndex.removeId(changes.getOldJobTitle(), userId);
                industryIndex.removeId(changes.getOldIndustry(), userId);
                r = true;
            }
        }
        else if (changes.isInsert()) {
            if (jobTitleIndex.containsId(changes.getOldJobTitle(), userId)
                    && industryIndex.containsId(changes.getOldIndustry(), userId)) {
                jobTitleIndex.putId(changes.getNewJobTitle(), userId);
                industryIndex.putId(changes.getNewIndustry(), userId);
                r = true;
            }
        }
        return r;
    }
}