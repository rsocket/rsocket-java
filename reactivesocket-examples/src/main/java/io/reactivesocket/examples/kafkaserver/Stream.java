package io.reactivesocket.examples.kafkaserver;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * A very trivial and inefficient implementation of a stream; our stream is a queue and each subscriber
 * to the stream has their own buffer
 */
public class Stream {

    // internal representation of the stream
    private Queue<Tuple<String, String>> list;

    // subscribers who have subscribed to this stream
    private ConcurrentMap<String, KafkaSubscription> subMap;

    // a buffer that stores data that is not sent, and waits for a subscriber to call request() for more data
    private ConcurrentMap<String, ConcurrentLinkedQueue<Tuple<String, String>>> bufferMap;

    public Stream() {
        this.list = new ConcurrentLinkedQueue<>();
        this.subMap = new ConcurrentHashMap<>();
        this.bufferMap = new ConcurrentHashMap<>();
    }

    // when an element gets added, we also push it to all the subscribers who are following this stream
    public void add(Tuple<String, String> tup) {
        this.list.add(tup);
        // We will send each subscriber the item that was inserted
        for (String key : subMap.keySet()) {
            try {
                KafkaSubscription tempsub = subMap.get(key);
                if (tempsub.isCanceled()) {
                    // if this subscription has been canceled, remove it
                    subMap.remove(key);
                    bufferMap.remove(key);
                    continue;
                }
                if (tempsub.canSend()) {
                    // if we can send the data successfully, then send it
                    tempsub.sendData(new PayloadImpl(tup.getK(), tup.getV()));
                } else {
                    // otherwise we will store it in the buffer
                    ConcurrentLinkedQueue<Tuple<String, String>> q = bufferMap.get(key);
                    if (q == null) {
                        q = new ConcurrentLinkedQueue<>();
                        q.add(tup);
                    }
                    bufferMap.put(key, q);
                }
            } catch (Exception e) {
                // if we have trouble writing to the socket, remove it
                subMap.remove(key);
            }
        }
    }


    public Iterator<Tuple<String, String>> getIter() {
        return this.list.iterator();
    }

    /**
     * Each client should have an id, so we have a map from a client to their subscription, and we can remove
     * them if they request to be unsubscribed
     * @param id the id of the client
     * @param sub the subscriber
     */
    public void addFunction(String id, KafkaSubscription sub) {
        this.subMap.put(id, sub);
    }

    /**
     * Effective unsubscribes a client from this stream
     * @param id the id of the client who wants to unsubscribe
     */
    public void removeFunction(String id) {
        this.subMap.remove(id);
    }

    /**
     *  This check whether a subscriber is subscribed to this stream
     * @param id the id of the subscriber
     * @return true if they are subscribed, false otherwise
     */
    public boolean isSubscribed(String id) {
        return this.subMap.get(id) != null;
    }

    /**
     * Call this function to notify the stream that a subscriber has requested more data
     * @param id the id of the subscriber requesting more data
     */
    public void notify(String id) {
        ConcurrentLinkedQueue<Tuple<String, String>> q = bufferMap.get(id);
        KafkaSubscription sub = subMap.get(id);
        if (q != null) {
            while (sub.canSend() && q.size() > 0) {
                Tuple<String, String> tup = q.poll();
                sub.sendData(new PayloadImpl(tup.getK(), tup.getV()));
            }
        }
    }

    /**
     * Unsubscribes the subscriber from this stream
     * @param id the id of the subscriber to unsubscribe
     */
    public void unsubscribe(String id) {
        this.subMap.remove(id);
        this.bufferMap.remove(id);
    }

}
