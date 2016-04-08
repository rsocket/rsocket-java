/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.local;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by rroeser on 4/2/16.
 */
class LocalReactiveSocketManager {
    private static final LocalReactiveSocketManager INSTANCE = new LocalReactiveSocketManager();

    private final ConcurrentHashMap<String, LocalServerDuplexConection> serverConnections;
    private final ConcurrentHashMap<String, LocalClientDuplexConnection> clientConnections;

    private LocalReactiveSocketManager() {
        serverConnections = new ConcurrentHashMap<>();
        clientConnections = new ConcurrentHashMap<>();
    }

    public static LocalReactiveSocketManager getInstance() {
        return INSTANCE;
    }

    public LocalClientDuplexConnection getClientConnection(String name) {
        return clientConnections.computeIfAbsent(name, LocalClientDuplexConnection::new);
    }

    public void removeClientConnection(String name) {
        clientConnections.remove(name);
    }

    public LocalServerDuplexConection getServerConnection(String name) {
        return serverConnections.computeIfAbsent(name, LocalServerDuplexConection::new);
    }

    public void removeServerDuplexConnection(String name) {
        serverConnections.remove(name);
    }

}
