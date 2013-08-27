package batchmode;

import java.util.*;

/**
 * The UserFieldIndex object indexes the given database by using its values (job_title or industry) as keys to look up
 * collections of associated database keys (user_id). Thus, the indexes created by this are inverted relative to the
 * database indexes. <p>
 *
 * While this does still have the problem of essentially storing n records, there are several points that
 * make this significantly better than storing n complete records: <p>
 * 1. The string field values are not necessarily unique, but user_id's are. No matter what, we were going to have to
 * store all of the user_ids, but using this, we are able to eliminate redundancy and store each string value only once.<p>
 * 2. Strings are much bigger than numbers. A <code>long</code> only takes up 16 bits while each character takes up 4
 * bits meaning that a length=5 field value is already larger than the user_id. Although the exact amount of space saved
 * is not completely predictable, we may assume that it is fairly significant since that is the purpose of batch update.<p>
 * 3. Although not strictly space saving, storing batches of user_ids by the values that are being updated does
 * significantly reduce the amount of work that needs to be done. This definitely is a boon to the speed of the application
 * and, to some degree, may possibly reduce use of auxiliary memory. (although I am skeptical) <p>
 * In summary, although this indexing still requires some coefficient of n in memory, it should be a much smaller coefficient.
 */
public final class UserFieldIndex {
    Map<String, Collection<Long>> index;

    public UserFieldIndex() {
        index = new HashMap<String, Collection<Long>>();
    }

    /**
     * Stores a collection of user ids with the same key
     * @param key a key to store the batch of ids by
     * @param batch a collection of keys to store
     */
    public void putBatch(String key, Collection<Long> batch) {
        Collection<Long> oldBatch = index.get(key);
        if (oldBatch==null) {
            oldBatch = new HashSet<Long>(batch);
            index.put(key, oldBatch);
        }
        oldBatch.addAll(batch);
    }

    /**
     * Removes all userIds associated with the key
     * @param key the key to remove from the index
     * @return a collection containing all of the user ids that were removed.
     */
    public Collection<Long> removeBatch(String key) {
        if (index.containsKey(key))
            return index.remove(key);
        else return Collections.emptySet();
    }

    /**
     * Replaces all instances of the old key with the new key in stored key-userid pairs. Affected entries will no longer
     * be retrievable with the old key and will only be obtained using the new key.
     * @ oldKey key of all user ids to be re-associated
     * @ newKey new key for user ids to be associated with
     */
    public Collection<Long> modifyBatchKey(String oldKey, String newKey) {
        //remove the batch from its old location
        Collection<Long> batch = removeBatch(oldKey);
        if(index.containsKey(newKey))
            index.get(newKey).addAll(batch);
        else
            index.put(newKey, batch);
        Collection<Long> batchCopy = new HashSet<Long>(batch);
        //put batch in its new location.
        putBatch(newKey, batch);
        return batchCopy;
    }

    /**
     * Stores a new key-userId pair in the index.
     */
    public void putId(String key, Long userId) {
        Collection<Long> batch = index.get(key);
        if (batch == null) {
            batch = new HashSet<Long>();
            index.put(key, batch);
        }
        batch.add(userId);
    }

    /**
     * Removes a key-id pair from this index and returns <code>true</code> if the pair is successfully removed.
     * @param key the key of the batch to search
     * @param userId the user id to be removed from the batch
     * @return <code>true</code> if the specified key-id could be found and removed; <code>false</code> otherwise
     */
    public boolean removeId(String key, Long userId) {
        if (!index.containsKey(key))
            return false;
        Collection<Long> batch = index.get(key);
        if (batch.size()<=1)
            index.remove(key);
        return batch.remove(userId);
    }

    /**
     * Replaces the <code>oldKey</code> association of the specified <code>userId</code> with the <code>newKey</code>
     * @param oldKey the key to be replaced
     * @param newKey the new key association
     * @param userId the userId whose key is to be replaced
     */
    public boolean modifyIdKey(String oldKey, String newKey, Long userId) {
        boolean r = removeId(oldKey, userId);
        if (r)
            putId(newKey, userId);
        return r;
    }

    /**
     * Returns a set containing all of the keys in this index
     * @return the set of all keys in the index
     */
    public Set<String> keySet() {
        return index.keySet();
    }

    /**
     * Returns <code>true</code> if the index contains any user ids for the specified key.
     * @param key the key to search for in the index
     * @return <code>true</code> if the index contains any user ids for the specified key; <code>false</code> otherwise
     */
    public boolean containsKey(String key) {
        if (!index.containsKey(key)) return false;
        else if (index.get(key).isEmpty()) return false;
        else return true;
    }

    /**
     * Checks to see if this index contains the given key-user id pair
     * @param key the key of the batch to search
     * @param userId the user id being searched for
     * @return <code>true</code> if the index contains the given key and user id pair; <code>false</code> otherwise
     */
    public boolean containsId(String key, Long userId) {
        if(index.containsKey(key)) {
            return index.get(key).contains(userId);
        }
        return false;
    }

    /**
     * Removes all entries from this index
     */
    public void clear() {
        index.clear();
    }
}
