package io;

import java.util.HashMap;

public interface DocumentUpdateQueue {
    /**
     * Adds a new subscription function object to this queue
     * @param callback a function object to be triggered whenever a document update is published to the queue.
     */
    public void subscribe_to_queue(DocumentUpdateQueueCallback callback);

    /**
     * The function object interface for subscribing to document updates from the queue system.
     */
    public interface DocumentUpdateQueueCallback {
        /**
         * This method is called whenever an update is posted to the queue.
         * @param userId the userId affected
         * @param changes a <code>HashMap</code> of
         */
        public void publish(long userId, HashMap<String, Object> changes);
    }
}
