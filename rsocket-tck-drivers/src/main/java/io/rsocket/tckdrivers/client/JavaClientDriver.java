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

package io.rsocket.tckdrivers.client;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.tckdrivers.common.ConsoleUtils;
import io.rsocket.tckdrivers.common.EchoSubscription;
import io.rsocket.tckdrivers.common.MySubscriber;
import io.rsocket.tckdrivers.common.ParseChannel;
import io.rsocket.tckdrivers.common.ParseChannelThread;
import io.rsocket.tckdrivers.common.ParseMarble;
import io.rsocket.tckdrivers.common.Tuple;
import io.rsocket.util.PayloadImpl;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * This class is the driver for the Java RSocket client. To use with class with the current Java
 * impl of RSocket, one should supply both a test file as well as a function that can generate
 * RSockets on demand. This driver will then parse through the test file, and for each test, it will
 * run them on their own thread and print out the results.
 */
public class JavaClientDriver {

  private final Map<String, MySubscriber<Payload>> payloadSubscribers;
  private final Map<String, MySubscriber<Void>> fnfSubscribers;
  private final Map<String, String> idToType;
  private final Supplier<RSocket> createClient;
  private final String AGENT = "[CLIENT]";
  private ConsoleUtils consoleUtils = new ConsoleUtils(AGENT);

  public JavaClientDriver(Supplier<RSocket> createClient)
      throws FileNotFoundException {
    this.payloadSubscribers = new HashMap<>();
    this.fnfSubscribers = new HashMap<>();
    this.idToType = new HashMap<>();
    this.createClient = createClient;
  }

  public enum TestResult {
    PASS,
    FAIL,
    CHANNEL
  }

  /**
   * Parses through the commands for each test, and calls handlers that execute the commands.
   *
   * @param test the list of strings which makes up each test case
   * @param name the name of the test
   * @return an option with either true if the test passed, false if it failed, or empty if no
   *     subscribers were found
   */
  public TestResult parse(List<String> test, String name) throws Exception {
    List<String> id = new ArrayList<>();
    Iterator<String> iter = test.iterator();
    boolean channelTest = false; // tells whether this is a test for channel or not
    boolean hasPassed = true;
    while (iter.hasNext()) {
      String line = iter.next();
      String[] args = line.split("%%");
      switch (args[0]) {
        case "subscribe":
          handleSubscribe(args);
          id.add(args[2]);
          break;
        case "channel":
          channelTest = true;
          handleChannel(args, iter, name, true);
          break;
        case "echochannel":
          handleEchoChannel(args);
          break;
        case "await":
          switch (args[1]) {
            case "terminal":
              hasPassed &= handleAwaitTerminal(args);
              break;
            case "atLeast":
              hasPassed &= handleAwaitAtLeast(args);
              break;
            case "no_events":
              hasPassed &= handleAwaitNoEvents(args);
              break;
            default:
              break;
          }
          break;

        case "assert":
          switch (args[1]) {
            case "no_error":
              hasPassed &= assertNoError(args);
              break;
            case "error":
              hasPassed &= assertError(args);
              break;
            case "received":
              hasPassed &= assertReceived(args);
              break;
            case "received_n":
              hasPassed &= assertReceivedN(args);
              break;
            case "received_at_least":
              hasPassed &= assertReceivedAtLeast(args);
              break;
            case "completed":
              hasPassed &= assertCompleted(args);
              break;
            case "no_completed":
              hasPassed &= assertNoCompleted(args);
              break;
            case "canceled":
              hasPassed &= assertCancelled(args);
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
          handleEOF();
          break;
        default:
          // the default behavior is to just skip the line, so we can acommodate slight changes to the TCK
          break;
      }
    }
    // this check each of the subscribers to see that they all passed their assertions
    if (id.size() > 0) {
      for (String str : id) {
        if (payloadSubscribers.get(str) != null)
          hasPassed = hasPassed && payloadSubscribers.get(str).hasPassed();
        else hasPassed = hasPassed && fnfSubscribers.get(str).hasPassed();
      }
      if (hasPassed) return TestResult.PASS;
      else return TestResult.FAIL;
    } else if (channelTest) return TestResult.CHANNEL;
    else throw new Exception("There is no subscriber in this test");
  }

  /**
   * This function takes in the arguments for the subscribe command, and subscribes an instance of
   * MySubscriber with an initial request of 0 (which means don't immediately make a request) to an
   * instance of the corresponding publisher
   *
   * @param args
   */
  private void handleSubscribe(String[] args) {
    switch (args[1]) {
      case "rr":
        MySubscriber<Payload> rrsub = new MySubscriber<>(0L, AGENT);
        payloadSubscribers.put(args[2], rrsub);
        idToType.put(args[2], args[1]);
        RSocket rrclient = createClient.get();
        consoleUtils.info("Sending RR with " + args[3] + " " + args[4]);
        Publisher<Payload> rrpub = rrclient.requestResponse(new PayloadImpl(args[3], args[4]));
        rrpub.subscribe(rrsub);
        break;
      case "rs":
        MySubscriber<Payload> rssub = new MySubscriber<>(0L, AGENT);
        payloadSubscribers.put(args[2], rssub);
        idToType.put(args[2], args[1]);
        RSocket rsclient = createClient.get();
        consoleUtils.info("Sending RS with " + args[3] + " " + args[4]);
        Publisher<Payload> rspub = rsclient.requestStream(new PayloadImpl(args[3], args[4]));
        rspub.subscribe(rssub);
        break;
      case "fnf":
        MySubscriber<Void> fnfsub = new MySubscriber<>(0L, AGENT);
        fnfSubscribers.put(args[2], fnfsub);
        idToType.put(args[2], args[1]);
        RSocket fnfclient = createClient.get();
        consoleUtils.info("Sending fnf with " + args[3] + " " + args[4]);
        Publisher<Void> fnfpub = fnfclient.fireAndForget(new PayloadImpl(args[3], args[4]));
        fnfpub.subscribe(fnfsub);
        break;
      default:
        break;
    }
  }

  /**
   * This function takes in an iterator that is parsing through the test, and collects all the parts
   * that make up the channel functionality. It then create a thread that runs the test, which we
   * wait to finish before proceeding with the other tests.
   *
   * @param args
   * @param iter
   * @param name
   */
  private void handleChannel(String[] args, Iterator<String> iter, String name, boolean pass) {
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
    MySubscriber<Payload> testsub = new MySubscriber<>(1L, AGENT);
    CountDownLatch c = new CountDownLatch(1);

    // we now create the publisher that the server will subscribe to with its own subscriber
    // we want to give that subscriber a subscription that the client will use to send data to the server
    RSocket client = createClient.get();
    AtomicReference<ParseChannelThread> mypct = new AtomicReference<>();
    Publisher<Payload> pub =
        client.requestChannel(
            new Publisher<Payload>() {
              @Override
              public void subscribe(Subscriber<? super Payload> s) {
                ParseMarble pm = new ParseMarble(s, AGENT);
                TestSubscription ts = new TestSubscription(pm, initialPayload, s);
                s.onSubscribe(ts);
                ParseChannel pc = new ParseChannel(commands, testsub, pm, name, pass, AGENT);
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
      consoleUtils.info("interrupted");
    }
    mypct.get().join();
  }

  /**
   * This handles echo tests. This sets up a channel connection with the EchoSubscription, which we
   * pass to the MySubscriber.
   *
   * @param args
   */
  private void handleEchoChannel(String[] args) {
    Payload initPayload = new PayloadImpl(args[1], args[2]);
    MySubscriber<Payload> testsub = new MySubscriber<>(1L, AGENT);
    RSocket client = createClient.get();
    Publisher<Payload> pub =
        client.requestChannel(
            new Publisher<Payload>() {
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

  private boolean handleAwaitTerminal(String[] args) {
    consoleUtils.info("Awaiting at Terminal");
    String id = args[2];
    if (idToType.get(id) == null) {
      consoleUtils.failure("Could not find subscriber with given id");
      return false;
    } else {
      if (idToType.get(id).equals("fnf")) {
        MySubscriber<Void> sub = fnfSubscribers.get(id);
        return sub.awaitTerminalEvent();
      } else {
        MySubscriber<Payload> sub = payloadSubscribers.get(id);
        return sub.awaitTerminalEvent();
      }
    }
  }

  private boolean handleAwaitAtLeast(String[] args) {
    consoleUtils.info("Awaiting at Terminal for at least " + args[3]);
    try {
      String id = args[2];
      MySubscriber<Payload> sub = payloadSubscribers.get(id);
      return sub.awaitAtLeast(Long.parseLong(args[3]));
    } catch (InterruptedException e) {
      consoleUtils.error("interrupted");
      return false;
    }
  }

  private boolean handleAwaitNoEvents(String[] args) {
    try {
      String id = args[2];
      MySubscriber<Payload> sub = payloadSubscribers.get(id);
      return sub.awaitNoEvents(Long.parseLong(args[3]));
    } catch (InterruptedException e) {
      consoleUtils.error("Interrupted");
      return false;
    }
  }

  private boolean assertNoError(String[] args) {
    String id = args[2];
    if (idToType.get(id) == null) {
      consoleUtils.error("Could not find subscriber with given id");
      return false;
    } else {
      if (idToType.get(id).equals("fnf")) {
        MySubscriber<Void> sub = fnfSubscribers.get(id);
        try {
          sub.assertNoErrors();
          return true;
        } catch (Throwable ex) {
          return false;
        }
      } else {
        MySubscriber<Payload> sub = payloadSubscribers.get(id);
        sub.assertNoErrors();
        try {
          sub.assertNoErrors();
          return true;
        } catch (Throwable ex) {
          return false;
        }
      }
    }
  }

  private boolean assertError(String[] args) {
    consoleUtils.info("Checking for error");
    String id = args[2];
    if (idToType.get(id) == null) {
      consoleUtils.error("Could not find subscriber with given id");
      return false;
    } else {
      if (idToType.get(id).equals("fnf")) {
        MySubscriber<Void> sub = fnfSubscribers.get(id);
        return sub.myAssertError(new Throwable());
      } else {
        MySubscriber<Payload> sub = payloadSubscribers.get(id);
        return sub.myAssertError(new Throwable());
      }
    }
  }

  private boolean assertReceived(String[] args) {
    consoleUtils.info("Verify we received " + args[3]);
    String id = args[2];
    MySubscriber<Payload> sub = payloadSubscribers.get(id);
    String[] values = args[3].split("&&");
    List<Tuple<String, String>> assertList = new ArrayList<>();
    for (String v : values) {
      String[] vals = v.split(",");
      assertList.add(new Tuple<>(vals[0], vals[1]));
    }
    return sub.assertValues(assertList);
  }

  private boolean assertReceivedN(String[] args) {
    String id = args[2];
    MySubscriber<Payload> sub = payloadSubscribers.get(id);
    try {
      sub.assertValueCount(Integer.parseInt(args[3]));
    } catch (Throwable ex) {
      return false;
    }
    return true;
  }

  private boolean assertReceivedAtLeast(String[] args) {
    String id = args[2];
    MySubscriber<Payload> sub = payloadSubscribers.get(id);
    return sub.assertReceivedAtLeast(Integer.parseInt(args[3]));
  }

  private boolean assertCompleted(String[] args) {
    consoleUtils.info("Handling onComplete");
    String id = args[2];
    if (idToType.get(id) == null) {
      consoleUtils.error("Could not find subscriber with given id");
      return false;
    } else {
      if (idToType.get(id).equals("fnf")) {
        MySubscriber<Void> sub = fnfSubscribers.get(id);
        try {
          sub.assertComplete();
        } catch (Throwable ex) {
          return false;
        }
        return true;
      } else {
        MySubscriber<Payload> sub = payloadSubscribers.get(id);
        try {
          sub.assertComplete();
        } catch (Throwable ex) {
          return false;
        }
        return true;
      }
    }
  }

  private boolean assertNoCompleted(String[] args) {
    consoleUtils.info("Handling NO onComplete");
    String id = args[2];
    if (idToType.get(id) == null) {
      consoleUtils.error("Could not find subscriber with given id");
      return false;
    } else {
      if (idToType.get(id).equals("fnf")) {
        MySubscriber<Void> sub = fnfSubscribers.get(id);
        try {
          sub.assertNotComplete();
        } catch (Throwable ex) {
          return false;
        }
        return true;
      } else {
        MySubscriber<Payload> sub = payloadSubscribers.get(id);
        try {
          sub.assertNotComplete();
        } catch (Throwable ex) {
          return false;
        }
        return true;
      }
    }
  }

  private boolean assertCancelled(String[] args) {
    String id = args[2];
    MySubscriber<Payload> sub = payloadSubscribers.get(id);
    return sub.isCancelled();
  }

  private void handleRequest(String[] args) {
    Long num = Long.parseLong(args[1]);
    String id = args[2];
    if (idToType.get(id) == null) {
      consoleUtils.error("Could not find subscriber with given id");
    } else {
      if (idToType.get(id).equals("fnf")) {
        MySubscriber<Void> sub = fnfSubscribers.get(id);
        consoleUtils.info("ClientDriver: Sending request for " + num);
        sub.request(num);
      } else {
        MySubscriber<Payload> sub = payloadSubscribers.get(id);
        consoleUtils.info("ClientDriver: Sending request for " + num);
        sub.request(num);
      }
    }
  }

  private void handleTake(String[] args) {
    String id = args[2];
    Long num = Long.parseLong(args[1]);
    MySubscriber<Payload> sub = payloadSubscribers.get(id);
    sub.take(num);
  }

  private void handleCancel(String[] args) {
    String id = args[1];
    MySubscriber<Payload> sub = payloadSubscribers.get(id);
    sub.cancel();
  }

  private void handleEOF() {
    MySubscriber<Void> fnfsub = new MySubscriber<>(0L, AGENT);
    RSocket fnfclient = createClient.get();
    Publisher<Void> fnfpub = fnfclient.fireAndForget(new PayloadImpl("shutdown", "shutdown"));
    fnfpub.subscribe(fnfsub);
    fnfsub.request(1);
  }

  /** A subscription for channel, it handles request(n) by sort of faking an initial payload. */
  private class TestSubscription implements Subscription {
    private boolean firstRequest = true;
    private ParseMarble pm;
    private Payload initPayload;
    private Subscriber<? super Payload> sub;

    public TestSubscription(ParseMarble pm, Payload initpayload, Subscriber<? super Payload> sub) {
      this.pm = pm;
      this.initPayload = initpayload;
      this.sub = sub;
    }

    @Override
    public void cancel() {
      pm.cancel();
    }

    @Override
    public void request(long n) {
      consoleUtils.info("TestSubscription: request " + n);
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
