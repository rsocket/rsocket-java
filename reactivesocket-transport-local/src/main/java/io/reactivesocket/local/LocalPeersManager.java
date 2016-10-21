/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.local;

import java.util.concurrent.ConcurrentHashMap;

final class LocalPeersManager {

    private static final ConcurrentHashMap<String, LocalServer> servers = new ConcurrentHashMap<>();

    private LocalPeersManager() {
        // No instances..
    }

    public static LocalServer getServerOrDie(String name) {
        LocalServer localServer = servers.get(name);
        if (localServer == null) {
            throw new IllegalArgumentException("No local servers registered with name: " + name);
        }
        return localServer;
    }

    public static void unregister(String name) {
        LocalServer removed = servers.remove(name);
        if (removed != null) {
            removed.shutdown();
        }
    }

    public static void register(LocalServer server) {
        String name = server.getName();
        LocalServer existing = servers.putIfAbsent(name, server);
        if (existing != null) {
            if (existing.isActive()) {
                throw new IllegalStateException("A server with name " + name + " already exists.");
            } else {
                servers.replace(name, server);
            }
        }
    }
}
