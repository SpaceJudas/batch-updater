package io;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * This class provides a basic implementation of the DocumentUpdateQueue for use in unit testing. In addition to
 * providing an implementation for the subscribe method, it also contains a simple method that forces the callback to
 * publish with a given input.
 */
public class DocumentUpdateQueueImpl implements DocumentUpdateQueue{
    private List<DocumentUpdateQueueCallback> subscriptions;
    private DbDriver db;

    /**
     * Basic constructor for the DocumentUpdateQueueImpl
     */
    public DocumentUpdateQueueImpl() {
        subscriptions = new LinkedList<DocumentUpdateQueueCallback>();
    }

    /**
     * Adds the <code>DocumentUpdateQueueCallback</code>
     * @param callback a function object to be triggered whenever a document update is published to the queue.
     */
    @Override
    public void subscribe_to_queue(DocumentUpdateQueueCallback callback) {
        subscriptions.add(callback);
    }

    /**
     * Forces the callback method to publish with the given input.
     */
    public void publish(long user_id, HashMap<String, Object> changes) {
        db.read(user_id);

        for(DocumentUpdateQueueCallback subscribedCallback : subscriptions) {
            subscribedCallback.publish(user_id, changes);
        }
    }
}