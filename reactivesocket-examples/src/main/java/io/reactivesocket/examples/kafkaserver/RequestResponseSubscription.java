package io.reactivesocket.examples.kafkaserver;

import io.reactivesocket.Payload;
import org.reactivestreams.Subscriber;

import java.util.UUID;

public class RequestResponseSubscription extends KafkaSubscription{


    // This is not needed for the request response class
    @Override
    public boolean canSend() {
        return false;
    }

    // this is also not needed
    @Override
    public void sendData(Payload p) {
        return;
    }

    private boolean canceled = false;
    private Payload payload;
    private Subscriber<? super Payload> s;

    public RequestResponseSubscription(Payload payload, Subscriber<? super Payload> s) {
        this.payload = payload;
        this.s = s;
    }

    @Override
    public void cancel() {
        this.canceled = true;
    }

    @Override
    public void request(long n) {
        if (n > 0 && !canceled) {
            try {
                // get the data sent
                // Since this is only for creating stream, we shouldn't need to check metadata
                String streamName = PayloadImpl.byteToString(payload.getData());
                String metaData = PayloadImpl.byteToString(payload.getMetadata());

                // if the metadata tells to create stream, then do so
                if (metaData.equals("create")) {
                    if (KafkaServer.streamMap.get(streamName) != null) {
                        // stream already exists, abort
                        s.onError(new StreamAlreadyExistsException());
                        return;
                    }
                    // we add this stream to the map
                    KafkaServer.streamMap.put(streamName, new Stream());
                    // onComplete on success
                    System.out.println("Stream " + streamName + " created");
                    s.onNext(new PayloadImpl("Stream Created", streamName));
                    s.onComplete();
                }
                // if it's the initial handshake, then give the client a unique id
                else if (metaData.equals("handshake")) {
                    String uid = UUID.randomUUID().toString();
                    System.out.println("accepted client : " + uid);
                    s.onNext(new PayloadImpl(uid, ""));
                    s.onComplete();
                }
                // if it's a list stream request
                else if (metaData.equals("list")) {
                    String toSend = "";
                    for (String key : KafkaServer.streamMap.keySet()) {
                        toSend += key + " ";
                    }
                    s.onNext(new PayloadImpl(toSend, ""));
                    s.onComplete();
                }
                // if user wants to list their own subscription
                else if (metaData.equals("sub")) {
                    String toSend = "";
                    for (String key : KafkaServer.streamMap.keySet()) {
                        if (KafkaServer.streamMap.get(key).isSubscribed(streamName)) {
                            toSend += key + " ";
                        }
                    }
                    s.onNext(new PayloadImpl(toSend, ""));
                    s.onComplete();
                }

            } catch (Exception e) {
                // if anything goes wrong, let the client know
                e.printStackTrace();
                s.onError(e);
            }
        }
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }
}
