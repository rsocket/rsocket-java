/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.javax.websocket;

import org.glassfish.tyrus.server.Server;

import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.server.ServerApplicationConfig;
import javax.websocket.server.ServerEndpointConfig;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Pong {
    public static class ApplicationConfig implements ServerApplicationConfig {
        @Override
        public Set<ServerEndpointConfig> getEndpointConfigs(Set<Class<? extends Endpoint>> endpointClasses) {
            Set<ServerEndpointConfig> cfgs = new HashSet<>();
            cfgs.add(ServerEndpointConfig.Builder
                .create(PongEndpoint.class, "/rs")
                .build());
            return cfgs;
        }

        @Override
        public Set<Class<?>> getAnnotatedEndpointClasses(Set<Class<?>> scanned) {
            return Collections.emptySet();
        }
    }

    public static void main(String... args) throws DeploymentException {
        byte[] response = new byte[1024];
        Random r = new Random();
        r.nextBytes(response);

        Server server = new Server("localhost", 8025, null, null, ApplicationConfig.class);
        server.start();

        // Tyrus spawns all of its threads as daemon threads so we need to prop open the JVM with a blocking call.
        CountDownLatch latch = new CountDownLatch(1);
        try {
            latch.await(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            System.out.println("Interrupted main thread");
        } finally {
            server.stop();
        }
        System.exit(0);
    }
}
