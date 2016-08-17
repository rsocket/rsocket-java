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

package io.reactivesocket.tckdrivers.server;

import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;

import java.util.concurrent.CountDownLatch;

/**
 * An example of how to run the JavaServerDriver using the ReactiveSocket server creation tool in Java.
 */
public class JavaTCPServer {

    private CountDownLatch mutex;

    public JavaTCPServer() {
        mutex = new CountDownLatch(1);
    }

    public void run(String realfile, int port) {

        String file = "reactivesocket-tck-drivers/src/main/resources/server.txt";

        if (realfile != null) {
            file = realfile;
        }

        TcpReactiveSocketServer server = TcpReactiveSocketServer.create(port);

        JavaServerDriver jsd =
                new JavaServerDriver(file, server, mutex);
        jsd.run();
    }

    /**
     * Blocks until the server has started
     */
    public void awaitStart() {
        try {
            mutex.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
