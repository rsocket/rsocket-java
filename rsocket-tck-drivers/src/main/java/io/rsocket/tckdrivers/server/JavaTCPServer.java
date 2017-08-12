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

import io.rsocket.Closeable;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import reactor.ipc.netty.tcp.TcpServer;

/** An example of how to run the JavaServerDriver using the RSocket server creation tool in Java. */
public class JavaTCPServer {
  public JavaTCPServer() {
  }

  public void run(File file, int port) throws IOException {
    TcpServerTransport server = TcpServerTransport.create(TcpServer.create(port));

    JavaServerDriver jsd = JavaServerDriver.read(file);

    Closeable socket = RSocketFactory.receive().acceptor(jsd.acceptor()).transport(server).start().block();

    socket.onClose().block(Duration.ofDays(365));
  }
}
