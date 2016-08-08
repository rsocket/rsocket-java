package io.reactivesocket.examples.kafkaserver;

import io.reactivesocket.Payload;
import org.reactivestreams.Subscriber;

import java.util.Iterator;

/**
 * This class implements a subscription object that we use in the case that the client
 * requests a channel connection.
 */
public class ChannelSubscription extends KafkaSubscription {

    private Subscriber<? super Payload> s;
    private long numSent = 0;
    private long demand = 0;
    private boolean canceled = false;

    public ChannelSubscription(Subscriber<? super Payload> s) {
        this.s = s;
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public boolean canSend() {
        return numSent < demand;
    }

    @Override
    public void sendData(Payload p) {
        this.s.onNext(p);
        numSent++;
    }

    @Override
    public void cancel() {
        this.canceled = true;
    }

    @Override
    public void request(long n) {
        this.demand = n;
        this.numSent = 0;
        Iterator<Tuple<String, String>> streamIter = KafkaServer.irc.getIter();
        while (streamIter.hasNext()) {
            if (canceled) {
                return;
            }
            if (!canSend()) {
                break;
            }
            Tuple<String, String> tup = streamIter.next();
            s.onNext(new PayloadImpl(tup.getK(), tup.getV()));
        }

    }
}
