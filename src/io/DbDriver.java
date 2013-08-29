package io;

import java.util.Iterator;
import java.util.Map;

/**
 * The DbDriver interface provides a library for performing operations on a key-value document store where numeric keys
 * are associated with JSON documents. This interface is used within this application as a placeholder for the actual
 * driver discussed within the original problem, and for testing purposes.
 */
public interface DbDriver {
    /**
     * Returns a JSON document that represents a set of user information
     * @return an object representing a JSON document
     */
    public Object read(Long user_id);

    /**
     * Allows you to completely replace/create user information by sending a JSON document. Returns the updated
     * version ID of the document.
     */
    public Long update(Long user_id, Object user_data);

    /**
     * Delete user data entirely
     */
    public void delete(Long user_id);

    /**
     * Iterate through all the rows of the database (buffered). Since the return value of this is not specified I am
     * assuming (fairly safely, I believe) that it through database entries and returns both their key (user_id) and
     * the associated JSON document.
     * @return an iterator for the database
     */
    public Iterator<Map.Entry<Long, Object>> scan();
    public Iterator<Map.Entry<Long, Object>> scan(int userId);
}
