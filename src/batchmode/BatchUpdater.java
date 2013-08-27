package batchmode;

import batchmode.data.DocumentUpdate;
import batchmode.data.User;
import io.DbDriver;
import io.DocumentUpdateQueue;
import io.DocumentUpdateQueue.DocumentUpdateQueueCallback;

import java.util.*;

/**
 * This implementation of BatchUpdater allows the user to queue up batch changeJobTitle and changeIndustry calls, which
 * will not be executed until the user calls startBatch(). Once startBatch() is called, it is assumed that BatchUpdater
 * is the only reader and writer to the database and that no external writes are being performed on the document update
 * queue. This implementation uses indexing as specified by <code>UserDocumentIndex</code>. Once the index is created,
 * it is never again updated from the database
 */
public class BatchUpdater {
    DbDriver dbDriver;
    UserDocumentIndex index;
    Map<Long, SortedSet<DocumentUpdate>> externalChanges;     //userId -> DocumentUpdate (sorted by version)
    Queue<BatchUpdatePair> industryBatchChanges;
    Queue<BatchUpdatePair> jobTitleBatchChanges;

    public BatchUpdater(DbDriver dbDriver, DocumentUpdateQueue documentUpdateQueue) {
        //Initialize
        this.dbDriver = dbDriver;
        index = new UserDocumentIndex(dbDriver);
        externalChanges = new HashMap<Long, SortedSet<DocumentUpdate>>();
        industryBatchChanges = new LinkedList<BatchUpdatePair>();
        jobTitleBatchChanges = new LinkedList<BatchUpdatePair>();
        //Subscribe to queue using anonymous class.
        documentUpdateQueue.subscribe_to_queue(
            new DocumentUpdateQueueCallback()
            {
                @Override
                public void publish(long userId, HashMap<String, Object> changes) {
                    Long userId_Obj = userId;
                    if (!externalChanges.containsKey(userId_Obj))
                        externalChanges.put(userId_Obj, new TreeSet<DocumentUpdate>());
                    SortedSet<DocumentUpdate> userChanges = externalChanges.get(userId_Obj);
                    userChanges.add(new DocumentUpdate(changes));
                }
            });
    }

    /**
     * Public batch update method that sets job title to <code>newJobTitle</code> in all user documents that currently
     * have the job title <code>oldJobTitle</code>. As is the case with all of this class' batch update methods, the
     * requested batch job title change will be queued until startBatch is called.
     * @param
     */
    public void changeJobTitle(String oldJobTitle, String newJobTitle) {
        jobTitleBatchChanges.offer(new BatchUpdatePair(oldJobTitle, newJobTitle));
    }

    /**
     * Public batch update method that sets industry to <code>newIndustry</code> in all user documents that currently
     * have the job title <code>oldIndustry</code>. As is the case with all of this class' batch update methods, the
     * requested batch job title change will be queued until startBatch is called.
     * @param
     */
    public void changeIndustry(String oldIndustry, String newIndustry) {
        industryBatchChanges.offer(new BatchUpdatePair(oldIndustry, newIndustry));
    }

    /**
     * Starts the batch
     */
    public void executeBatchUpdates() {
        applyQueuedExternalChanges();
        applyQueuedBatchChanges();
    }

    /**
     * Applies all queued changes from external sources (obtained from the callback on the DocumentUpdateQueue) to the
     * <code>BatchUpdater</code> index.
     */
    private void applyQueuedExternalChanges() {
        for(Long userId : externalChanges.keySet()) {
            SortedSet<DocumentUpdate> documentUpdates = externalChanges.get(userId);
            for (DocumentUpdate update : documentUpdates) {
                System.out.println(update.getVersion());
                index.updateId(userId, update);
            }
        }
        externalChanges.clear();
    }
    /**
     * Applies all queued batch updates changes (obtained via the changeIndustry and changeJobTitle methods) to the
     * <code>BatchUpdater</code> index.
     */
    private void applyQueuedBatchChanges() {
    //Personal note: the loop in this method could probably be moved to an external method, thus reducing the amount of
    //redundant code
        Collection<Long> ids;
        while(!industryBatchChanges.isEmpty()) {
            BatchUpdatePair industryUpdate = industryBatchChanges.poll();
            //Update the index entries and get the associated user ids
            ids = index.changeIndustry(industryUpdate.getOldValue(), industryUpdate.getNewValue());
            //Update the database
            for (Long id : ids) {
                User user = new User(dbDriver.read(id).toString());
                user.setIndustry(industryUpdate.getNewValue());
                dbDriver.update(id, user.toJsonString());
            }
        }
        industryBatchChanges.clear();

        while(!jobTitleBatchChanges.isEmpty()) {
            BatchUpdatePair jobTitleUpdate = jobTitleBatchChanges.poll();
            //Update the index entries and get the associated user ids
            ids = index.changeJobTitle(jobTitleUpdate.getOldValue(), jobTitleUpdate.getNewValue());
            //Update the database entries
            for (Long id : ids) {
                User user = new User(dbDriver.read(id).toString());
                user.setJobTitle(jobTitleUpdate.getNewValue());
                dbDriver.update(id, user.toJsonString());
            }
        }
        jobTitleBatchChanges.clear();
    }

    private class BatchUpdatePair {
        String oldValue, newValue;

        //Private default constructor forces implementors to use the full constructor
        private BatchUpdatePair() {}

        public BatchUpdatePair(String oldValue, String newValue) {
            this.oldValue = oldValue; this.newValue = newValue;
        }
        public String getOldValue() {
            return oldValue;
        }
        public void setOldValue(String oldValue) {
            this.oldValue = oldValue;
        }
        public String getNewValue() {
            return newValue;
        }
        public void setNewValue(String newValue) {
            this.newValue = newValue;
        }
    }
}