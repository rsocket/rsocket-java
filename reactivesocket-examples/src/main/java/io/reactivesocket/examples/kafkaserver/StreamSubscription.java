package io.reactivesocket.examples.kafkaserver;

import io.reactivesocket.Payload;
import org.reactivestreams.Subscriber;

import java.util.Iterator;

public class StreamSubscription extends KafkaSubscription {

    // this will keep track of the number of elements sent
    private long numSent = 0;
    private long demand = 0;
    private boolean canceled = false;
    private Payload payload;
    private Subscriber<? super Payload> s;
    private boolean firstRequest = true;
    private Stream myStream;
    private String uid;

    public StreamSubscription(Payload payload, Subscriber<? super Payload> s) {
        this.payload = payload;
        this.s = s;
    }

    @Override
    public void cancel() {
        this.canceled = true;
        if (myStream != null) {
            // if we have subscribed to a stream, cancel the subscription
            myStream.unsubscribe(this.uid);
        }
    }

    @Override
    public void request(long n) {
        this.demand = n;
        this.numSent = 0;
        String streamName = PayloadImpl.byteToString(payload.getData());
        this.uid = PayloadImpl.byteToString(payload.getMetadata());
        // if this is the first time the client has requested information
        if (firstRequest) {
            firstRequest = false;

            Stream stream = KafkaServer.streamMap.get(streamName);
            myStream = stream;

            // if the stream name does not exist, return the exception to the subscriber
            if (stream == null) {
                s.onError(new StreamNotFoundException());
                return;
            }
            System.out.println(uid + " subscribed to " + streamName);
            // otherwise, send all existing items to the subscriber
            Iterator<Tuple<String, String>> streamIter = stream.getIter();
            while (streamIter.hasNext()) {
                if (canceled) {
                    return;
                }
                if (!canSend()) {
                    // If the client doesn't request more than in the iterator, it will skip the rest of
                    // the stored data and go directly to the stream the next time the client calls request()
                    // No elegant way at the moment to fix this behavior
                    break;
                }
                Tuple<String, String> tup = streamIter.next();
                sendData(new PayloadImpl(tup.getK(), tup.getV()));
            }
            // now we give the subscriber to the stream object
            stream.addFunction(uid, this);
        } else {
            // otherwise we should notify the stream that we need to send more data
            myStream.notify(uid);
        }
    }

    @Override
    public boolean canSend() {
        return numSent < demand;
    }

    private void incrementSent() {
        numSent++;
    }

    @Override
    public void sendData(Payload p) {
        this.s.onNext(p);
        incrementSent();
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }
}
