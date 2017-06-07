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

import io.rsocket.AbstractRSocket;
import io.rsocket.Closeable;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.tckdrivers.common.ConsoleUtils;
import io.rsocket.tckdrivers.common.EchoSubscription;
import io.rsocket.tckdrivers.common.MySubscriber;
import io.rsocket.tckdrivers.common.ParseChannel;
import io.rsocket.tckdrivers.common.ParseChannelThread;
import io.rsocket.tckdrivers.common.ParseMarble;
import io.rsocket.tckdrivers.common.ParseThread;
import io.rsocket.tckdrivers.common.Tuple;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** This is the driver for the server. */
public class JavaServerDriver {

  // these map initial payload -> marble, which dictates the behavior of the server
  private Map<Tuple<String, String>, String> requestResponseMarbles;
  private Map<Tuple<String, String>, String> requestStreamMarbles;
  private Map<Tuple<String, String>, String> requestSubscriptionMarbles;
  // channel doesn't have an initial payload, but maybe the first payload sent can be viewed as the "initial"
  private Map<Tuple<String, String>, List<String>> requestChannelCommands;
  private Set<Tuple<String, String>> requestChannelFail;
  private Set<Tuple<String, String>> requestEchoChannel;
  // first try to implement single channel subscriber
  private BufferedReader reader;
  // the instance of the server so we can shut it down
  private TcpServerTransport server;
  private Closeable serverCloseable;
  private CountDownLatch waitStart;
  private ConsoleUtils consoleUtils = new ConsoleUtils("[SERVER]");

  public JavaServerDriver(String path) {
    requestResponseMarbles = new HashMap<>();
    requestStreamMarbles = new HashMap<>();
    requestSubscriptionMarbles = new HashMap<>();
    requestChannelCommands = new HashMap<>();
    requestEchoChannel = new HashSet<>();
    try {
      reader = new BufferedReader(new FileReader(path));
    } catch (Exception e) {
      consoleUtils.error("File not found");
    }
    requestChannelFail = new HashSet<>();
  }

  // should be used if we want the server to be shutdown upon receiving some EOF packet
  public JavaServerDriver(String path, TcpServerTransport server, CountDownLatch waitStart) {
    this(path);
    this.server = server;
    this.waitStart = waitStart;
    requestChannelFail = new HashSet<>();
  }

  /** Starts up the server */
  public void run() {
    this.parse();
    this.serverCloseable =
        RSocketFactory.receive()
            .acceptor(new SocketAcceptorImpl())
            .transport(this.server)
            .start()
            .block();
    this.waitStart.countDown();
    serverCloseable.onClose().block();
  }

  class SocketAcceptorImpl implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      AbstractRSocket abstractRSocket =
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(
                  s -> {
                    try {
                      MySubscriber<Payload> sub = new MySubscriber<>(0L, "[SERVER]");
                      payloads.subscribe(sub);
                      // want to get equivalent of "initial payload" so we can route behavior, this might change in the future
                      sub.request(1);
                      sub.awaitAtLeast(1);
                      Tuple<String, String> initpayload =
                          new Tuple<>(sub.getElement(0).getK(), sub.getElement(0).getV());
                      consoleUtils.initialPayload(
                          "Received Channel " + initpayload.getK() + " " + initpayload.getV());
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
                            10000); // request a large number, which basically means the client can send whatever
                      } else {
                        consoleUtils.error(
                            "Request channel payload "
                                + initpayload.getK()
                                + " "
                                + initpayload.getV()
                                + "has no handler");
                      }

                    } catch (Exception e) {
                      consoleUtils.failure("Interrupted");
                    }
                  });
            }

            @Override
            public final Mono<Void> fireAndForget(Payload payload) {
              return Mono.from(
                  s -> {
                    Tuple<String, String> initialPayload =
                        new Tuple<>(
                            StandardCharsets.UTF_8.decode(payload.getData()).toString(),
                            StandardCharsets.UTF_8.decode(payload.getMetadata()).toString());
                    consoleUtils.initialPayload(
                        "Received firenforget "
                            + initialPayload.getK()
                            + " "
                            + initialPayload.getV());
                    if (initialPayload.getK().equals("shutdown")
                        && initialPayload.getV().equals("shutdown")) {
                      try {
                        Thread.sleep(2000);
                        serverCloseable.close().subscribe();
                      } catch (Exception e) {
                        e.printStackTrace();
                      }
                    }
                  });
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.from(
                  s -> {
                    Tuple<String, String> initialPayload =
                        new Tuple<>(
                            StandardCharsets.UTF_8.decode(payload.getData()).toString(),
                            StandardCharsets.UTF_8.decode(payload.getMetadata()).toString());
                    String marble = requestResponseMarbles.get(initialPayload);
                    consoleUtils.initialPayload(
                        "Received requestresponse "
                            + initialPayload.getK()
                            + " "
                            + initialPayload.getV());
                    if (marble != null) {
                      ParseMarble pm = new ParseMarble(marble, s, "[SERVER]");
                      s.onSubscribe(new TestSubscription(pm));
                      new ParseThread(pm).start();
                    } else {
                      consoleUtils.failure(
                          "Request response payload "
                              + initialPayload.getK()
                              + " "
                              + initialPayload.getV()
                              + "has no handler");
                    }
                  });
            }

            @Override
            public Flux<Payload> requestStream(Payload payload) {
              return Flux.from(
                  s -> {
                    Tuple<String, String> initialPayload =
                        new Tuple<>(
                            StandardCharsets.UTF_8.decode(payload.getData()).toString(),
                            StandardCharsets.UTF_8.decode(payload.getMetadata()).toString());
                    String marble = requestStreamMarbles.get(initialPayload);
                    consoleUtils.initialPayload(
                        "Received Stream " + initialPayload.getK() + " " + initialPayload.getV());
                    if (marble != null) {
                      ParseMarble pm = new ParseMarble(marble, s, "[SERVER]");
                      s.onSubscribe(new TestSubscription(pm));
                      new ParseThread(pm).start();
                    } else {
                      consoleUtils.failure(
                          "Request stream payload "
                              + initialPayload.getK()
                              + " "
                              + initialPayload.getV()
                              + "has no handler");
                    }
                  });
            }
          };

      return Mono.just(abstractRSocket);
    }
  }

  /**
   * This function parses through each line of the server handlers and primes the supporting data
   * structures to be prepared for the first request. We return a RequestHandler object, which tells
   * the RSocket server how to handle each type of request. The code inside the RequestHandler is
   * lazily evaluated, and only does so before the first request. This may lead to a sort of bug,
   * where getting concurrent requests as an initial request will nondeterministically lead to some
   * data structures to not be initialized.
   *
   * @return a RequestHandler that details how to handle each type of request.
   */
  public void parse() {
    try {
      String line = reader.readLine();
      while (line != null) {
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
            handleChannel(args, reader);
          case "echochannel":
            requestEchoChannel.add(new Tuple<>(args[1], args[2]));
            break;
          default:
            break;
        }

        line = reader.readLine();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * This handles the creation of a channel handler, it basically groups together all the lines of
   * the channel script and put it in a map for later access
   *
   * @param args
   * @param reader
   * @throws IOException
   */
  private void handleChannel(String[] args, BufferedReader reader) throws IOException {
    Tuple<String, String> initialPayload = new Tuple<>(args[1], args[2]);
    if (args.length == 5) {
      // we know that this test should fail
      requestChannelFail.add(initialPayload);
    }
    String line = reader.readLine();
    List<String> commands = new ArrayList<>();
    while (!line.equals("}")) {
      commands.add(line);
      line = reader.readLine();
    }
    requestChannelCommands.put(initialPayload, commands);
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
