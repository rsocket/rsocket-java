package io.reactivesocket.examples.kafkaclient;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.*;
import io.reactivesocket.internal.frame.ByteBufferUtil;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static rx.RxReactiveStreams.toObservable;

public class KafkaClient {

    // the identifier of this client to the server
    private String uid;

    // We will use this subscriber when we are sending command to create stream
    private Subscriber<Payload> createSubscriber;

    // This is a trivial fire n forget subscriber we use to send messages to log
    private Subscriber<Void> sendMessageSubscriber;

    // This is a subscriber for a stream
    private Subscriber<Payload> streamSubscriber;

    // This is a subscriber for a channel
    private Subscriber<Payload> channelSubscriber;

    // This is a subscriber for the initial handshake
    private Subscriber<Payload> handshakeSubscriber;

    // This is a subscriber used to list all the streams
    private Subscriber<Payload> listSubscriber;

    private ReactiveSocket client = null;

    // keeps track of the streams we are subscribed to
    private Map<String, Subscription> subscriptions;


    public static void main(String[] args) {
        KafkaClient kc = new KafkaClient();
        kc.run();
    }

    // We will initialize all of our handlers in this constructor
    public KafkaClient() {
        try {
            subscriptions = new HashMap<>();
            String target = "tcp://localhost:4567/rs";
            URI uri = new URI(target);

            client = createClient(uri);

            // We use our SimpleSubscriber Builder to avoid boilerplate code
            // This subscriber shakes the servers hand
            this.handshakeSubscriber = new SimpleSubscriber.Builder().withOnNext(payload -> {
                        KafkaClient.this.uid = ByteBufferUtil.toUtf8String(payload.getData());
                        System.out.println("uid : " + ByteBufferUtil.toUtf8String(payload.getData()));
                    })
                    .withOnError(throwable -> {System.out.println("Failed to handshake");}).build();

            // Notifies user when the stream has been created
            this.createSubscriber = new SimpleSubscriber.Builder().withOnNext(payload -> {})
                    .withOnError(throwable -> System.out.println("Could not subscribe")).build();

            // should just print out each element of the stream as it comes
            // maybe can make this more refined in the future by saving stream
            // elements somewhere
            this.streamSubscriber = new Subscriber<Payload>() {
                private Subscription s;
                @Override
                public void onSubscribe(Subscription s) {
                    this.s = s;
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Payload payload) {
                    subscriptions.put(ByteBufferUtil.toUtf8String(payload.getMetadata()), s);
                    System.out.println("data : " + ByteBufferUtil.toUtf8String(payload.getData()) + " | stream : "
                            + ByteBufferUtil.toUtf8String(payload.getMetadata()));
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("Failed to subscribe");
                }

                @Override
                public void onComplete() {
                }
            };


            // This shouldn't really do anything, just fires a message
            // We can't use the Builder because this Subscriber is of type void
            this.sendMessageSubscriber = new Subscriber<Void>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Void aVoid) {
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("Sorry, your message could not be sent");
                }

                @Override
                public void onComplete() {
                }
            };


            this.channelSubscriber = new SimpleSubscriber.Builder().withOnNext(payload -> {
                        System.out.println(ByteBufferUtil.toUtf8String(payload.getMetadata()) + " : "
                                + ByteBufferUtil.toUtf8String(payload.getData()));
                    })
                    .withOnError(throwable -> throwable.printStackTrace()).build();


            // this subscriber allows us to list the existing streams
            this.listSubscriber = new SimpleSubscriber.Builder().withOnNext(payload -> {
                        String[] streams = ByteBufferUtil.toUtf8String(payload.getData()).split(" ");
                        for (String stream : streams) {
                            System.out.println(stream);
                        }
                    })
                    .withOnError(throwable -> {System.out.println("Could not list streams");}).build();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // This function will take care of the repl for the client
    public void run() {
        // do our initial handshake
        Publisher<Payload> handshakePublisher = client.requestResponse(new PayloadImpl("", "handshake"));
        handshakePublisher.subscribe(this.handshakeSubscriber);
        System.out.println("Welcome to a simple Kafka implementation with reactive sockets");
        System.out.println("type \"help\" for usage");
        while (true) {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            try {
                String input = br.readLine();
                String[] inputElements = input.split(" ");
                if (inputElements.length == 0) {
                    continue;
                }
                String command = inputElements[0];
                if (command.equals("create")) {
                    create(inputElements);
                }
                else if (command.equals("subscribe")) {
                    stream(inputElements);
                }
                else if (command.equals("add")) {
                    add(inputElements);
                }
                else if (command.equals("unsubscribe")) {
                    unsubscribe(inputElements);
                }
                else if (command.equals("chat")) {
                    channel(inputElements);
                }
                else if (command.equals("ls")) {
                    listStream(inputElements);
                }
                else if (command.equals("help")) {
                    System.out.println("Usage: create <stream name> : creates a stream of the given name");
                    System.out.println("subscribe <stream name> : subscribes to the stream of the given name");
                    System.out.println("add <stream name> <data> : adds the data to the stream");
                    System.out.println("unsubscribe <stream name> : unsubscribes from the given stream");
                    System.out.println("chat <username> : joins an irc like chat using the given username");
                    System.out.println("ls [sub]: \"ls\" lists the current streams while \"ls sub\" lists the streams" +
                            " you are subscribed to");
                } else {
                    System.out.println("Sorry, could not parse command. Type \"help\" for options");
                }
            } catch (Exception e) {
                System.out.println("Something went wrong");
            }
        }
    }

    private void listStream(String[] arguments) {
        // we create a publisher requests a list of the current streams
        if (arguments.length == 1) {
            Publisher<Payload> listPublisher = client.requestResponse(new PayloadImpl("", "list"));
            listPublisher.subscribe(listSubscriber);
        } else if (arguments[1].equals("sub")) {
            Publisher<Payload> listPublisher = client.requestResponse(new PayloadImpl(uid, "sub"));
            listPublisher.subscribe(listSubscriber);
        }
    }

    private void create(String[] arguments) {
        // we create a publisher that sends a create message request
        Publisher<Payload> createPublisher = client.requestResponse(new PayloadImpl(arguments[1], "create"));
        createPublisher.subscribe(this.createSubscriber);
    }

    private void stream(String[] arguments) {
        // we create a publisher that sends a stream request
        Publisher<Payload> streamPublisher = client.requestStream(new PayloadImpl(arguments[1], this.uid));
        streamPublisher.subscribe(this.streamSubscriber);
    }

    private void add(String[] arguments) {
        // we create a publisher that sends a fire n forget add request
        Publisher<Void> addPublisher = client.fireAndForget(
                new PayloadImpl(String.join(" ", Arrays.copyOfRange(arguments, 2, arguments.length)), arguments[1]));
        addPublisher.subscribe(this.sendMessageSubscriber);
    }

    private void unsubscribe(String[] arguments) {
        // we should just cancel the subscription
        subscriptions.get(arguments[1]).cancel();
        subscriptions.remove(arguments[1]);
    }

    private void channel(String[] arguments) {
        String username = arguments[1];
        Publisher<Payload> cPublisher = client.requestChannel(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                // enter repl
                System.out.println("Welcome to the chat. Type \".quit\" to quit the chat.");
                s.onNext(new PayloadImpl("Welcome", username + " has joined the chat"));
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    String input = reader.readLine();
                    while (!input.equals(".quit")) {
                        s.onNext(new PayloadImpl(input, username));
                        input = reader.readLine();
                        if (input.equals(".quit")) {
                            s.onNext(new PayloadImpl("goodbye", username + " has quit"));
                            s.onComplete();
                        }
                    }
                    input = "";
                } catch (Exception e) {
                    // let server know if there is an exception
                    s.onError(e);
                }
            }
        });
        cPublisher.subscribe(channelSubscriber);
    }
    /**
     * A function that creates a ReactiveSocket on a new TCP connection.
     * @return a ReactiveSocket
     */
    public static ReactiveSocket createClient(URI uri) throws URISyntaxException {
        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("", "");
        if ("tcp".equals(uri.getScheme())) {
            Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory =
                    socketAddress -> TcpClient.newClient(socketAddress);

            return toObservable(
                    TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace, clientFactory)
                            .connect(new InetSocketAddress(uri.getHost(), uri.getPort()))).toSingle()
                    .toBlocking()
                    .value();
        }
        else {
            throw new UnsupportedOperationException("uri unsupported: " + uri);
        }
    }

}
