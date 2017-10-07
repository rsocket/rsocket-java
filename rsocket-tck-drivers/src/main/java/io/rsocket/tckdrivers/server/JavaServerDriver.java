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

package io.rsocket.tckdrivers.server;

import static com.google.common.io.Files.readLines;
import static org.junit.Assert.*;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.tckdrivers.common.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** This is the driver for the server. */
public class JavaServerDriver {

  // these map initial payload -> marble, which dictates the behavior of the server
  private Map<Tuple<String, String>, String> requestResponseMarbles = new HashMap<>();
  private Map<Tuple<String, String>, String> requestStreamMarbles = new HashMap<>();
  private Map<Tuple<String, String>, String> requestSubscriptionMarbles = new HashMap<>();
  // channel doesn't have an initial payload, but maybe the first payload sent can be viewed as the
  // "initial"
  private Map<Tuple<String, String>, List<String>> requestChannelCommands = new HashMap<>();
  private Set<Tuple<String, String>> requestChannelFail = new HashSet<>();
  private Set<Tuple<String, String>> requestEchoChannel = new HashSet<>();
  private ConsoleUtils consoleUtils = new ConsoleUtils("[SERVER]");

  // should be used if we want the server to be shutdown upon receiving some EOF packet
  public JavaServerDriver() {}

  public SocketAcceptor acceptor() {
    return (setup, sendingSocket) -> {
      AbstractRSocket abstractRSocket =
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(
                  s -> {
                    try {
                      MySubscriber<Payload> sub = new MySubscriber<>(0L, "[SERVER]");
                      payloads.subscribe(sub);
                      // want to get equivalent of "initial payload" so we can route behavior, this
                      // might change in the future
                      sub.request(1);
                      sub.awaitAtLeast(1);
                      Tuple<String, String> initpayload =
                          new Tuple<>(sub.getElement(0).getK(), sub.getElement(0).getV());
                      consoleUtils.initialPayload(
                          "Received Channel " + initpayload.getK() + " " + initpayload.getV());
                      String msg = "Request channel payload " + "%s " + "%s " + "has no handler";
                      assertTrue(
                          String.format(msg, initpayload.getK(), initpayload.getV()),
                          (requestChannelCommands.containsKey(initpayload)
                              || requestEchoChannel.contains(initpayload)));
                      // if this is a normal channel handler, then initiate the normal setup
                      if (requestChannelCommands.containsKey(initpayload)) {
                        ParseMarble pm = new ParseMarble(s, "[SERVER]");
                        s.onSubscribe(new TestSubscription(pm));
                        ParseChannel pc;
                        if (requestChannelFail.contains(initpayload))
                          pc =
                              new ParseChannel(
                                  requestChannelCommands.get(initpayload),
                                  sub,
                                  pm,
                                  "CHANNEL",
                                  false,
                                  "[SERVER]");
                        else
                          pc =
                              new ParseChannel(
                                  requestChannelCommands.get(initpayload), sub, pm, "[SERVER]");
                        ParseChannelThread pct = new ParseChannelThread(pc);
                        pct.start();
                      } else if (requestEchoChannel.contains(initpayload)) {
                        EchoSubscription echoSubscription = new EchoSubscription(s);
                        s.onSubscribe(echoSubscription);
                        sub.setEcho(echoSubscription);
                        sub.request(
                            10000); // request a large number, which basically means the client can
                        // send whatever
                      }
                    } catch (Exception e) {
                      assertNull("interrupted ", e.getMessage());
                    }
                  });
            }

            @Override
            public final Mono<Void> fireAndForget(Payload payload) {
              return Mono.from(
                  s -> {
                    Tuple<String, String> initialPayload =
                        new Tuple<>(payload.getDataUtf8(), payload.getMetadataUtf8());
                    consoleUtils.initialPayload(
                        "Received firenforget "
                            + initialPayload.getK()
                            + " "
                            + initialPayload.getV());
                  });
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.from(
                  s -> {
                    Tuple<String, String> initialPayload =
                        new Tuple<>(payload.getDataUtf8(), payload.getMetadataUtf8());
                    String marble = requestResponseMarbles.get(initialPayload);
                    consoleUtils.initialPayload(
                        "Received requestresponse "
                            + initialPayload.getK()
                            + " "
                            + initialPayload.getV());
                    String msg = "Request stream payload " + " %s" + " %s" + " has no handler";
                    assertNotNull(
                        String.format(msg, initialPayload.getK(), initialPayload.getV()), marble);
                    ParseMarble pm = new ParseMarble(marble, s, "[SERVER]");
                    s.onSubscribe(new TestSubscription(pm));
                    new ParseThread(pm).start();
                  });
            }

            @Override
            public Flux<Payload> requestStream(Payload payload) {
              return Flux.from(
                  s -> {
                    Tuple<String, String> initialPayload =
                        new Tuple<>(payload.getDataUtf8(), payload.getMetadataUtf8());
                    String marble = requestStreamMarbles.get(initialPayload);
                    consoleUtils.initialPayload(
                        "Received Stream " + initialPayload.getK() + " " + initialPayload.getV());
                    String msg = "Request stream payload " + " %s" + " %s" + " has no handler";
                    assertNotNull(
                        String.format(msg, initialPayload.getK(), initialPayload.getV()), marble);
                    ParseMarble pm = new ParseMarble(marble, s, "[SERVER]");
                    s.onSubscribe(new TestSubscription(pm));
                    new ParseThread(pm).start();
                  });
            }
          };

      return Mono.just(abstractRSocket);
    };
  }

  /**
   * This function parses through each line of the server handlers and primes the supporting data
   * structures to be prepared for the first request. We return a RequestHandler object, which tells
   * the RSocket server how to handle each type of request. The code inside the RequestHandler is
   * lazily evaluated, and only does so before the first request. This may lead to a sort of bug,
   * where getting concurrent requests as an initial request will nondeterministically lead to some
   * data structures to not be initialized.
   *
   * @param lines the lines of test script
   */
  public void parse(List<String> lines) {
    Iterator<String> lineIterator = lines.iterator();
    while (lineIterator.hasNext()) {
      String line = lineIterator.next();
      String[] args = line.split("%%");
      switch (args[0]) {
        case "rr":
          // put the request response marble in the hash table
          requestResponseMarbles.put(new Tuple<>(args[1], args[2]), args[3]);
          break;
        case "rs":
          requestStreamMarbles.put(new Tuple<>(args[1], args[2]), args[3]);
          break;
        case "sub":
          requestSubscriptionMarbles.put(new Tuple<>(args[1], args[2]), args[3]);
          break;
        case "channel":
          handleChannel(args, lineIterator);
          break;
        case "echochannel":
          handleChannel(args, lineIterator);
          requestEchoChannel.add(new Tuple<>(args[1], args[2]));
          break;
        default:
          break;
      }
    }
  }

  /**
   * This handles the creation of a channel handler, it basically groups together all the lines of
   * the channel script and put it in a map for later access
   *
   * @param args
   * @param reader
   */
  private void handleChannel(String[] args, Iterator<String> reader) {
    Tuple<String, String> initialPayload = new Tuple<>(args[1], args[2]);
    if (args.length == 5) {
      // we know that this test should fail
      requestChannelFail.add(initialPayload);
    }
    String line = reader.next();
    List<String> commands = new ArrayList<>();
    while (!line.equals("}")) {
      commands.add(line);
      line = reader.next();
    }
    requestChannelCommands.put(initialPayload, commands);
  }

  public static JavaServerDriver read(File file) throws IOException {
    List<String> lines = readLines(file, StandardCharsets.UTF_8);
    JavaServerDriver driver = new JavaServerDriver();
    driver.parse(lines);
    return driver;
  }

  /** A trivial subscription used to interface with the ParseMarble object */
  private class TestSubscription implements Subscription {
    private ParseMarble pm;

    public TestSubscription(ParseMarble pm) {
      this.pm = pm;
    }

    @Override
    public void cancel() {
      pm.cancel();
    }

    @Override
    public void request(long n) {
      consoleUtils.info("TestSubscription: request received for " + n);
      pm.request(n);
    }
  }
}
