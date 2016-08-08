package io.reactivesocket.examples.kafkaserver;

import io.reactivesocket.Payload;
import org.reactivestreams.Subscription;

/**
 * This defines the interface for our subscription object in this project
 * In this case, our subscription object handles all the logic of sending
 * data through the reactive socket
 */
public abstract class KafkaSubscription implements Subscription {

    /**
     * Should check to see if we are still allowed to send data
     * @return true if we can send data, false otherwise
     */
    public abstract boolean canSend();

    /**
     * Tells this subscription object to send data
     * @param p the payload to send
     */
    public abstract void sendData(Payload p);

    /**
     *
     * @return true if this subscription is canceled
     */
    public abstract boolean isCanceled();

}
