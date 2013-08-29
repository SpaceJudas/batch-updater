package incomplete;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * WARNING: THIS CLASS IS INCOMPLETE AND VERY BAD AT RESOURCE MANAGEMENT; USE AT YOUR OWN RISK
 *
 * The UserFieldFileIndex object indexes the given database by using its values (job_title or industry) as keys to look up
 * collections of associated database keys (user_id). Thus, the indexes created by this are inverted relative to the
 * database indexes. <p>
 *
 * This is essentially a reimplementation of the UserFieldIndex that writes the index to the hard drive as a series of
 * files to reduce memory use. Since index reads and writes require the files to be loaded up, this index runs more
 * slowly, even though it uses less heap space.
 */
public final class UserFieldFileIndex {
    static long indexSerial = 1l;
    final long mySerial;
    final int maxFileSize;
    int currentFileSize;
    long currentFileId;

    Map<String, Collection<Long>> index;
    Map<String, Long> fileIndex;

    public UserFieldFileIndex() {
        index = new HashMap<String, Collection<Long>>();
        fileIndex = new HashMap<String, Long>();
        mySerial = indexSerial;
        indexSerial++;
        maxFileSize = 200;
        currentFileSize = 0;
        currentFileId = 1;
    }
    public UserFieldFileIndex(int maxFileSize) {
        index = new HashMap<String, Collection<Long>>();
        fileIndex = new HashMap<String, Long>();
        mySerial = indexSerial;
        indexSerial++;
        this.maxFileSize = maxFileSize;
        currentFileSize = 0;
        currentFileId = 1;
    }

    public File getIndexFile(String key) {
        System.out.println(System.getProperty("user.dir")+getFileName(key));
        return new File(System.getProperty("user.dir")+getFileName(key));
    }

    public String getFileName(String key) {
        if(fileIndex.containsKey(key))
            return "idx-"+mySerial+"-"+fileIndex.get(key);
        else if (currentFileSize<maxFileSize) {
            currentFileSize++;
            fileIndex.put(key, currentFileId);
            return "idx-"+mySerial+"-"+currentFileId;
        }
        else {
            currentFileId++;
            currentFileSize=0;
            fileIndex.put(key, currentFileId);
            return "idx-"+mySerial+"-"+currentFileId;
        }
    }

    public static void main(String[] args) {
        try {
            removeEntry("lemon party", new File("idx-1-1.dat"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Collection<Long> removeEntry(String key, File in) throws IOException {
        System.out.println(System.getProperty("user.dir")+"/idx-temp.dat");
        File tempIn = new File(System.getProperty("user.dir")+"/idx-temp.dat");
        System.out.println(tempIn.getAbsolutePath());

        BufferedReader reader = new BufferedReader(new FileReader(in));
        PrintWriter tempWriter = new PrintWriter(tempIn);

        String line = null;
        String batchStr = null;
        while ((line = reader.readLine()) != null) {
            int seperatorIndex = line.indexOf(':');
            if (line.substring(0,seperatorIndex).equals(key)) {
                batchStr = line.substring(seperatorIndex + 1);
            }
            else tempWriter.println(line);
        }
        // TODO: Write proper resource handling. In its current state, this method likely leaks memory
        in.delete();
        tempIn.renameTo(in);
        reader.close();
        tempWriter.close();
        //Build batch out of the line string
        String[] ids = batchStr.split(",");
        Collection<Long> batch = new HashSet<Long>();
        for (String s: ids) {
            batch.add(Long.parseLong(s));
        }
        return batch;
    }
    //TODO: add false returns in failure cases and double check for proper resource handling
    public static boolean putEntry(String key, Collection<Long> userIds, File in) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(in,true));
        writer.println();
        writer.print(key+":");
        for(Long id : userIds) {
            writer.print(id.longValue());
        }
        writer.close();
        return true;
    }
    //TODO: finish this method and merge it together with putEntry to make a single putBatch method
    public boolean putNewBatch(String key) {
        if (currentFileSize<maxFileSize) {
            currentFileSize++;
            fileIndex.put(key, currentFileId);
            String file = "idx-"+mySerial+"-"+currentFileId;
        }
        else {
            currentFileId++;
            currentFileSize=0;
            fileIndex.put(key, currentFileId);
            String file =  "idx-"+mySerial+"-"+currentFileId;
        }
        return false;
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
