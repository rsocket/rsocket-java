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

package io.rsocket.tckdrivers.common;

import io.rsocket.tckdrivers.server.JavaTCPServer;

public class ServerThread implements Runnable {
  private Thread t;
  private int port;
  private String serverfile;
  private JavaTCPServer server;

  public ServerThread(int port, String serverfile) {
    t = new Thread(this);
    this.port = port;
    this.serverfile = serverfile;
    server = new JavaTCPServer();
  }

  @Override
  public void run() {
    server.run(serverfile, port);
  }

  public void start() {
    t.start();
  }

  public void awaitStart() {
    server.awaitStart();
  }
}
