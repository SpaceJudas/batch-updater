package io;

import java.util.*;

/**
 * This class provides a basic implementation of the DbDriver interface for use in unit testing. This class uses a
 * HashMap (java.lang.Long --> Object [Object is assumed to have a toString() method that produces a JSON string) to
 * simulate the described document store in local memory. This class also contains methods that force exceptions to be
 * thrown so that exception handling may be tested.
 */
public class DbDriverImpl implements DbDriver {
    private HashMap<Long, Object> data;
    private LinkedList<Integer> countdowns;

    public DbDriverImpl() {
        data = new HashMap<Long, Object>();
        countdowns = new LinkedList<Integer>();
    }

    /**
     * Adds an exception to be thrown when the given int countdown reaches zero
     */
    public void addExceptionTest(int countdown) throws ConcurrentModificationException{
        countdowns.add(countdown);
    }

    /**
     * Decrements all countdown integers. When a count reaches 0 or below this throws a new
     * ConcurrentModificationException
     */
    public void countdown() {
        boolean throwException = false;
        for(int i = 0; i<countdowns.size(); i++) {
            Integer count = countdowns.get(i);
            countdowns.set(i, count-1);
            System.out.println(count);
            if (count<=0) {
                throwException = true;
                countdowns.remove(i);
            }
        }
        if (throwException) {
            throw new ConcurrentModificationException("Exception Testing!!!!");
        }
    }



    /**
     * Simulates database read. Calls countdown and then reads and returns record.
     */
    public Object read(Long user_id) {
        countdown();
        return data.get(user_id);
    }
    /**
     * Simulates database update. Calls countdown and then performs update.
     */
    public Long update(Long user_id, Object user_data) {
        countdown();
        long newVersion = 1l;
        /*if (data.containsKey(user_id))
            newVersion+=data.get(user_id).get("version").getAsLong();
        user_data.addProperty("version", newVersion);*/
        data.put(user_id, user_data);
        return user_id;
    }

    /**
     * Simulates database delete. Calls countdown and then removes record.
     */
    public void delete(Long user_id) {
        countdown();
        data.remove(user_id);
    }

    /**
     * Scan has been modified to return an anonymous iterator based on data.entrySet().iterator() that uses countdown
     * to throw ConcurrentModificationExceptions
     * @return
     */
    public Iterator<Map.Entry<Long, Object>> scan() {
        return new Iterator<Map.Entry<Long, Object>>() {
            Iterator<Map.Entry<Long, Object>> mapIterator = data.entrySet().iterator();
            @Override public boolean hasNext() { return mapIterator.hasNext(); }
            @Override public void remove() { mapIterator.remove(); }
            @Override public Map.Entry<Long, Object> next() {
                countdown();
                return mapIterator.next();
            }
        };
    }
}