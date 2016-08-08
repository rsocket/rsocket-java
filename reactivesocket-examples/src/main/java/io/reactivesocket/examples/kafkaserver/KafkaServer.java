package io.reactivesocket.examples.kafkaserver;

import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class KafkaServer {

    // This is a hashmap that keeps track of the names to the list representing streams
    protected static ConcurrentMap<String, Stream> streamMap = new ConcurrentHashMap<>();

    // This is a public chat stream that anyone can join, sort of like an irc
    protected static Stream irc = new Stream();

    public static void main(String[] args) {

        TcpReactiveSocketServer.create(4567).start((setupPayload, reactiveSocket) -> {
            return new RequestHandler.Builder().withRequestResponse(requestResponseFunction)
                    .withRequestStream(requestStreamFunction)
                    .withFireAndForget(fireForgetFunction)
                    .withRequestChannel(requestChannelFunction)
                    .build();
        }).awaitShutdown();

    }

    // we define our handlers here instead of inline to make things a lot more readable

    // Following the reactive streams protocol, we create a subscription and allow the client
    // to request data. This is a bit trivial since only 1 response will be sent, but
    // we must honor the agreement between publishers and subscribers
    private static Function<Payload, Publisher<Payload>> requestResponseFunction = payload -> s -> {
        KafkaSubscription subscription = new RequestResponseSubscription(payload, s);
        s.onSubscribe(subscription);
    };


    // When a stream is requested, we first loop through everything that is already in the stream
    // Next, we pass in the onNext method so that the stream itself can call onNext whenever
    // there is an update to it
    // If the stream doesn't exist, we call onError to let the client know
    private static Function<Payload, Publisher<Payload>> requestStreamFunction = payload -> s -> {
        KafkaSubscription subscription = new StreamSubscription(payload, s);
        s.onSubscribe(subscription);
    };

    // The metadata will encode the name of the stream to insert into
    // the data field will encode the actual data to insert
    private static Function<Payload, Publisher<Void>> fireForgetFunction = payload -> s -> {
        String streamName = PayloadImpl.byteToString(payload.getMetadata());
        String data = PayloadImpl.byteToString(payload.getData());

        // we get the steam of the name from the map, if it doesn't exist then it doesn't get added
        Stream stream = streamMap.get(streamName);
        if (stream == null) {
            s.onError(new StreamNotFoundException());
            return;
        }
        // add the data and forget about it
        stream.add(new Tuple<>(data, streamName));
        System.out.println(data + " added to " + streamName);
        s.onComplete();
    };


    private static Function<Publisher<Payload>, Publisher<Payload>> requestChannelFunction = payload -> s -> {

        System.out.println("client joined chat");
        // send all existing items to the subscriber
        KafkaSubscription subscription = new ChannelSubscription(s);
        s.onSubscribe(subscription);


        payload.subscribe(new Subscriber<Payload>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Payload payload) {
                if (PayloadImpl.byteToString(payload.getMetadata()).endsWith("has joined the chat")) {
                    // now we give this subscriber to the stream object
                    irc.addFunction(PayloadImpl.byteToString(payload.getMetadata()).split(" ")[0], subscription);
                }
                if (PayloadImpl.byteToString(payload.getMetadata()).endsWith("has quit")) {
                    irc.removeFunction(PayloadImpl.byteToString(payload.getMetadata()).split(" ")[0]);
                }
                irc.add(new Tuple<>(PayloadImpl.byteToString(payload.getData()),
                        PayloadImpl.byteToString(payload.getMetadata())));
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("irc error");
            }

            @Override
            public void onComplete() {
                return;
            }
        });
    };
}
