/*
 * Copyright 2016 Facebook, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.tckdrivers.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.tckdrivers.common.*;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This class is the driver for the Java ReactiveSocket client. To use with class with the current Java impl of
 * ReactiveSocket, one should supply both a test file as well as a function that can generate ReactiveSockets on demand.
 * This driver will then parse through the test file, and for each test, it will run them on their own thread and print
 * out the results.
 */
public class JavaClientDriver {

    // colors for printing things out
    private final String ANSI_RESET = "\u001B[0m";
    private final String ANSI_RED = "\u001B[31m";
    private final String ANSI_GREEN = "\u001B[32m";

    private final BufferedReader reader;
    private final Map<String, TestSubscriber<Payload>> payloadSubscribers;
    private final Map<String, TestSubscriber<Void>> fnfSubscribers;
    private final Map<String, String> idToType;
    private final Supplier<ReactiveSocket> createClient;

    public JavaClientDriver(String path, Supplier<ReactiveSocket> createClient) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(path));
        this.payloadSubscribers = new HashMap<>();
        this.fnfSubscribers = new HashMap<>();
        this.idToType = new HashMap<>();
        this.createClient = createClient;
    }

    /**
     * Splits the test file into individual tests, and then run each of them on their own thread.
     * @throws IOException
     */
    public void runTests() throws IOException {
        List<List<String>> tests = new ArrayList<>();
        List<String> test = new ArrayList<>();
        String line = reader.readLine();
        while (line != null) {
            switch (line) {
                case "!":
                    tests.add(test);
                    test = new ArrayList<>();
                    break;
                default:
                    test.add(line);
                    break;
            }
            line = reader.readLine();
        }
        tests.add(test);
        tests = tests.subList(1, tests.size()); // remove the first list, which is empty
        for (List<String> t : tests) {
            TestThread thread = new TestThread(t);
            thread.start();
            thread.join();
        }
    }

    /**
     * A wrapper for parse if a test is not named.
     * @param test
     * @return
     */
    private Optional<Boolean> parse(List<String> test) {
        return parse(test, "");
    }


    /**
     * Parses through the commands for each test, and calls handlers that execute the commands.
     * @param test the list of strings which makes up each test case
     * @param name the name of the test
     * @return an option with either true if the test passed, false if it failed, or empty if no subscribers were found
     */
    private Optional<Boolean> parse(List<String> test, String name) {
        List<String> id = new ArrayList<>();
        Iterator<String> iter = test.iterator();
        while (iter.hasNext()) {
            String line = iter.next();
            String[] args = line.split("%%");
            switch (args[0]) {
                case "subscribe":
                    handleSubscribe(args);
                    id.add(args[2]);
                    break;
                case "channel":
                    handleChannel(args, iter, name);
                    break;
                case "echochannel":
                    handleEchoChannel(args);
                    break;
                case "await":
                    switch (args[1]) {
                        case "terminal":
                            handleAwaitTerminal(args);
                            break;
                        case "atLeast":
                            handleAwaitAtLeast(args);
                            break;
                        case "no_events":
                            handleAwaitNoEvents(args);
                            break;
                        default:
                            break;
                    }
                    break;

                case "assert":
                    switch (args[1]) {
                        case "no_error":
                            handleNoError(args);
                            break;
                        case "error":
                            handleError(args);
                            break;
                        case "received":
                            handleReceived(args);
                            break;
                        case "received_n":
                            handleReceivedN(args);
                            break;
                        case "received_at_least":
                            handleReceivedAtLeast(args);
                            break;
                        case "completed":
                            handleCompleted(args);
                            break;
                        case "no_completed":
                            handleNoCompleted(args);
                            break;
                        case "canceled":
                            handleCancelled(args);
                            break;
                    }
                    break;
                case "take":
                    handleTake(args);
                    break;
                case "request":
                    handleRequest(args);
                    break;
                case "cancel":
                    handleCancel(args);
                    break;
                case "EOF":

                    break;
                default:

                    break;
            }

        }
        // this check each of the subscribers to see that they all passed their assertions
        if (id.size() > 0) {
            boolean hasPassed = true;
            for (String str : id) {
                if (payloadSubscribers.get(str) != null) hasPassed = hasPassed && payloadSubscribers.get(str).hasPassed();
                else hasPassed = hasPassed && fnfSubscribers.get(str).hasPassed();
            }
            return Optional.of(hasPassed);
        }
        else return Optional.empty();
    }

    private void handleSubscribe(String[] args) {
        switch (args[1]) {
            case "rr":
                TestSubscriber<Payload> rrsub = new TestSubscriber<>(0L);
                payloadSubscribers.put(args[2], rrsub);
                idToType.put(args[2], args[1]);
                ReactiveSocket rrclient = createClient.get();
                Publisher<Payload> rrpub = rrclient.requestResponse(new PayloadImpl(args[3], args[4]));
                rrpub.subscribe(rrsub);
                break;
            case "rs":
                TestSubscriber<Payload> rssub = new TestSubscriber<>(0L);
                payloadSubscribers.put(args[2], rssub);
                idToType.put(args[2], args[1]);
                ReactiveSocket rsclient = createClient.get();
                Publisher<Payload> rspub = rsclient.requestStream(new PayloadImpl(args[3], args[4]));
                rspub.subscribe(rssub);
                break;
            case "sub":
                TestSubscriber<Payload> rsubsub = new TestSubscriber<>(0L);
                payloadSubscribers.put(args[2], rsubsub);
                idToType.put(args[2], args[1]);
                ReactiveSocket rsubclient = createClient.get();
                Publisher<Payload> rsubpub = rsubclient.requestSubscription(new PayloadImpl(args[3], args[4]));
                rsubpub.subscribe(rsubsub);
                break;
            case "fnf":
                TestSubscriber<Void> fnfsub = new TestSubscriber<>(0L);
                fnfSubscribers.put(args[2], fnfsub);
                idToType.put(args[2], args[1]);
                ReactiveSocket fnfclient = createClient.get();
                Publisher<Void> fnfpub = fnfclient.fireAndForget(new PayloadImpl(args[3], args[4]));
                fnfpub.subscribe(fnfsub);
                break;
            default:break;
        }
    }

    /**
     * This function takes in an iterator that is parsing through the test, and collects all the parts that make up
     * the channel functionality. It then create a thread that runs the test, which we wait to finish before proceeding
     * with the other tests.
     * @param args
     * @param iter
     * @param name
     */
    private void handleChannel(String[] args, Iterator<String> iter, String name) {
        List<String> commands = new ArrayList<>();
        String line = iter.next();
        // channel script should be bounded by curly braces
        while (!line.equals("}")) {
            commands.add(line);
            line = iter.next();
        }
        // set the initial payload
        Payload initialPayload = new PayloadImpl(args[1], args[2]);

        // this is the subscriber that will request data from the server, like all the other test subscribers
        TestSubscriber<Payload> testsub = new TestSubscriber<>(1L);
        ParseChannel superpc = null;
        CountDownLatch c = new CountDownLatch(1);

        // we now create the publisher that the server will subscribe to with its own subscriber
        // we want to give that subscriber a subscription that the client will use to send data to the server
        ReactiveSocket client = createClient.get();
        PCTWrapper mypct = new PCTWrapper();
        Publisher<Payload> pub = client.requestChannel(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                ParseMarble pm = new ParseMarble(s);
                TestSubscription ts = new TestSubscription(pm, initialPayload, s);
                s.onSubscribe(ts);
                ParseChannel pc = new ParseChannel(commands, testsub, pm, name);
                ParseChannelThread pct = new ParseChannelThread(pc);
                pct.start();
                mypct.set(pct);
                c.countDown();
            }
        });
        pub.subscribe(testsub);
        try {
            c.await();
        } catch (InterruptedException e) {
            System.out.println("interrupted");
        }
        mypct.get().join();
    }

    /**
     * Trivial class that wraps around the ParseChannelThread
     */
    private class PCTWrapper {
        ParseChannelThread p;
        public void set(ParseChannelThread p) {
            this.p = p;
        }
        public ParseChannelThread get() {
            return this.p;
        }
    }

    /**
     * This handles echo tests. This sets up a channel connection with the EchoSubscription, which we pass to
     * the TestSubscriber.
     * @param args
     */
    private void handleEchoChannel(String[] args) {
        Payload initPayload = new PayloadImpl(args[1], args[2]);
        TestSubscriber<Payload> testsub = new TestSubscriber<>(1L);
        ReactiveSocket client = createClient.get();
        Publisher<Payload> pub = client.requestChannel(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                EchoSubscription echoSub = new EchoSubscription(s);
                s.onSubscribe(echoSub);
                testsub.setEcho(echoSub);
                s.onNext(initPayload);
            }
        });
        pub.subscribe(testsub);
    }

    private void handleAwaitTerminal(String[] args) {
        String id = args[2];
        if (idToType.get(id) == null) {
            System.out.println("Could not find subscriber with given id");
        } else {
            if (idToType.get(id).equals("fnf")) {
                TestSubscriber<Void> sub = fnfSubscribers.get(id);
                sub.awaitTerminalEvent();
            } else {
                TestSubscriber<Payload> sub = payloadSubscribers.get(id);
                sub.awaitTerminalEvent();
            }
        }
    }

    private void handleAwaitAtLeast(String[] args) {
        try {
            String id = args[2];
            TestSubscriber<Payload> sub = payloadSubscribers.get(id);
            sub.awaitAtLeast(Long.parseLong(args[3]));
        } catch (InterruptedException e) {
            System.out.println("interrupted");
        }
    }

    private void handleAwaitNoEvents(String[] args) {
        try {
            String id = args[2];
            TestSubscriber<Payload> sub = payloadSubscribers.get(id);
            sub.awaitNoEvents(Long.parseLong(args[3]));
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
        }
    }

    private void handleNoError(String[] args) {
        String id = args[2];
        if (idToType.get(id) == null) {
            System.out.println("Could not find subscriber with given id");
        } else {
            if (idToType.get(id).equals("fnf")) {
                TestSubscriber<Void> sub = fnfSubscribers.get(id);
                sub.assertNoErrors();
            } else {
                TestSubscriber<Payload> sub = payloadSubscribers.get(id);
                sub.assertNoErrors();
            }
        }
    }

    private void handleError(String[] args) {
        String id = args[2];
        if (idToType.get(id) == null) {
            System.out.println("Could not find subscriber with given id");
        } else {
            if (idToType.get(id).equals("fnf")) {
                TestSubscriber<Void> sub = fnfSubscribers.get(id);
                sub.assertError(new Throwable());
            } else {
                TestSubscriber<Payload> sub = payloadSubscribers.get(id);
                sub.assertError(new Throwable());
            }
        }
    }

    private void handleCompleted(String[] args) {
        String id = args[2];
        if (idToType.get(id) == null) {
            System.out.println("Could not find subscriber with given id");
        } else {
            if (idToType.get(id).equals("fnf")) {
                TestSubscriber<Void> sub = fnfSubscribers.get(id);
                sub.assertComplete();
            } else {
                TestSubscriber<Payload> sub = payloadSubscribers.get(id);
                sub.assertComplete();
            }
        }
    }

    private void handleNoCompleted(String[] args) {
        String id = args[2];
        if (idToType.get(id) == null) {
            System.out.println("Could not find subscriber with given id");
        } else {
            if (idToType.get(id).equals("fnf")) {
                TestSubscriber<Void> sub = fnfSubscribers.get(id);
                sub.assertNotComplete();
            } else {
                TestSubscriber<Payload> sub = payloadSubscribers.get(id);
                sub.assertNotComplete();
            }
        }
    }

    private void handleRequest(String[] args) {
        Long num = Long.parseLong(args[1]);
        String id = args[2];
        if (idToType.get(id) == null) {
            System.out.println("Could not find subscriber with given id");
        } else {
            if (idToType.get(id).equals("fnf")) {
                TestSubscriber<Void> sub = fnfSubscribers.get(id);
                sub.request(num);
            } else {
                TestSubscriber<Payload> sub = payloadSubscribers.get(id);
                sub.request(num);
            }
        }
    }

    private void handleTake(String[] args) {
        String id = args[2];
        Long num = Long.parseLong(args[1]);
        TestSubscriber<Payload> sub = payloadSubscribers.get(id);
        sub.take(num);
    }

    private void handleReceived(String[] args) {
        String id = args[2];
        TestSubscriber<Payload> sub = payloadSubscribers.get(id);
        String[] values = args[3].split("&&");
        if (values.length == 1) {
            String[] temp = values[0].split(",");
            sub.assertValue(new Tuple<>(temp[0], temp[1]));
        } else if (values.length > 1) {
            List<Tuple<String, String>> assertList = new ArrayList<>();
            for (String v : values) {
                String[] vals = v.split(",");
                assertList.add(new Tuple<>(vals[0], vals[1]));
            }
            sub.assertValues(assertList);
        }
    }

    private void handleReceivedN(String[] args) {
        String id = args[2];
        TestSubscriber<Payload> sub = payloadSubscribers.get(id);
        sub.assertValueCount(Integer.parseInt(args[3]));
    }

    private void handleReceivedAtLeast(String[] args) {
        String id = args[2];
        TestSubscriber<Payload> sub = payloadSubscribers.get(id);
        sub.assertReceivedAtLeast(Integer.parseInt(args[3]));
    }

    private void handleCancel(String[] args) {
        String id = args[1];
        TestSubscriber<Payload> sub = payloadSubscribers.get(id);
        sub.cancel();
    }

    private void handleCancelled(String[] args) {
        String id = args[2];
        TestSubscriber<Payload> sub = payloadSubscribers.get(id);
        sub.isCancelled();
    }

    /**
     * This thread class parses through a single test and prints whether it succeeded or not
     */
    private class TestThread implements Runnable {
        private Thread t;
        private List<String> test;

        public TestThread(List<String> test) {
            this.t = new Thread(this);
            this.test = test;
        }

        @Override
        public void run() {
            String name = "";
            if (test.get(0).startsWith("name")) {
                name = test.get(0).split("%%")[1];
                System.out.println("Starting test " + name);
                Optional<Boolean> finish = parse(test.subList(1, test.size()), name);
                if (!finish.isPresent()) return;
                if (finish.get()) System.out.println(ANSI_GREEN + name + " passed" + ANSI_RESET);
                else System.out.println(ANSI_RED + name + " failed" + ANSI_RESET);
            } else {
                System.out.println("Starting test");
                Optional<Boolean> finish = parse(test);
                if (!finish.isPresent()) return;
                if (finish.get()) System.out.println(ANSI_GREEN + "Test passed" + ANSI_RESET);
                else System.out.println(ANSI_RED + "Test failed" + ANSI_RESET);
            }
        }

        public void start() {t.start();}

        public void join() {
            try {
                t.join();
            } catch(Exception e) {
                System.out.println("join exception");
            }
        }

    }

    /**
     * A subscription for channel, it handles request(n) by sort of faking an initial payload.
     */
    private class TestSubscription implements Subscription {
        private boolean firstRequest = true;
        private ParseMarble pm;
        private Payload initPayload;
        private Subscriber<? super Payload> sub;

        public TestSubscription(ParseMarble pm, Payload initpayload, Subscriber<? super Payload> sub) {
            this.pm = pm;
            this.initPayload = initpayload;
            this. sub = sub;
        }

        @Override
        public void cancel() {
            pm.cancel();
        }

        @Override
        public void request(long n) {
            long m = n;
            if (firstRequest) {
                sub.onNext(initPayload);
                firstRequest = false;
                m = m - 1;
            }
            if (m > 0) pm.request(m);
        }
    }

}
