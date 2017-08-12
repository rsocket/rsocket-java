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
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.TckClientTest;
import io.rsocket.tckdrivers.server.JavaTCPServer;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  public static String host;

  @Option(name = "--port", description = "The port")
  public static int port;

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
  public static String tests;

  /**
   * A function that parses the test file, and run each test
   *
   * @param host client connects with this host
   * @param port client connects on this port
   * @param testsList list of tests to run
   */
  public static void runTests(String host, int port, List<String> testsList)
      throws Exception {
    //for (TckClientTest t : JavaClientDriver.extractTests(file)) {
    //  if (testsList.contains(t.name)) {
    //    try {
    //      JavaClientDriver jd = new JavaClientDriver("tcp://" + host + ":" + port + "/rs");
    //      jd.runTest(t.testLines(), t.name);
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //    }
    //  }
    //}
  }

  public static void main(String[] args) throws IOException {
    SingleCommand<Main> cmd = SingleCommand.singleCommand(Main.class);
    cmd.parse(args);
    boolean passed = true;

    if (server) {
      if (file == null) {
        file = new File("rsocket-tck-drivers/src/test/resources/servertest.txt");
      }
      new JavaTCPServer().run(file, port);
    } else if (client) {
      if (file == null) {
        file = new File("rsocket-tck-drivers/src/test/resources/clienttest.txt");
      }
      try {
        if (tests != null) {
          runTests(host, port, Arrays.asList(tests.split(",")));
        } else {
          runTests(host, port, new ArrayList<>());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
