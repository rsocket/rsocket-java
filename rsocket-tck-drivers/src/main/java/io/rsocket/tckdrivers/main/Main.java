/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.tckdrivers.main;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.TckClientTest;
import io.rsocket.tckdrivers.server.JavaServerDriver;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpServer;

/** This class is used to run both the server and the client, depending on the options given */
@Command(
  name = "rsocket-driver",
  description = "This runs the client and servers that use the driver"
)
public class Main {

  @Option(name = "--server", description = "set if you want to run the server")
  public static boolean server;

  @Option(name = "--client", description = "set if you want to run the client")
  public static boolean client;

  @Option(name = "--host", description = "The host to connect to for the client")
  public static String host = "localhost";

  @Option(name = "--port", description = "The port")
  public static int port = 30006;

  @Option(
    name = "--file",
    description =
        "The script file to parse, make sure to give the client and server the " + "correct files"
  )
  public static File file;

  @Option(
    name = "--tests",
    description =
        "For the client only, optional argument to list out the tests you"
            + " want to run, should be comma separated names"
  )
  public static List<String> tests;

  /**
   * A function that parses the test file, and run each test
   *
   * @param host client connects with this host
   * @param port client connects on this port
   * @param testFilter predicate for tests to run
   */
  public static void runTests(String host, int port, Predicate<String> testFilter) {
    TcpClientTransport transport = TcpClientTransport.create(host, port);
    Mono<RSocket> clientBuilder = RSocketFactory.connect().transport(transport).start();

    System.out.println("test file: " + file);

    for (TckClientTest t : TckClientTest.extractTests(file)) {
      if (testFilter.test(t.name)) {
        System.out.println("Running " + t.name);
        JavaClientDriver jd = new JavaClientDriver(clientBuilder);
        jd.runTest(t);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    SingleCommand<Main> cmd = SingleCommand.singleCommand(Main.class);
    cmd.parse(args);

    if (server) {
      if (file == null) {
        file = new File("rsocket-tck-drivers/src/test/resources/servertest.txt");
      }
      runServer();
    } else if (client) {
      if (file == null) {
        file = new File("rsocket-tck-drivers/src/test/resources/clienttest.txt");
      }
      try {
        if (tests != null) {
          runTests(host, port, name -> tests.contains(name));
        } else {
          runTests(host, port, t -> true);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void runServer() throws IOException {
    TcpServerTransport server = TcpServerTransport.create(TcpServer.create(port));

    JavaServerDriver jsd = JavaServerDriver.read(file);

    Closeable socket =
        RSocketFactory.receive().acceptor(jsd.acceptor()).transport(server).start().block();

    socket.onClose().block(Duration.ofDays(365));
  }
}
