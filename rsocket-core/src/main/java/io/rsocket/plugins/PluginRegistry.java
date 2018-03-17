/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.plugins;

import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import java.util.ArrayList;
import java.util.List;

public class PluginRegistry {
  private List<DuplexConnectionInterceptor> connections = new ArrayList<>();
  private List<RSocketInterceptor> clients = new ArrayList<>();
  private List<RSocketInterceptor> servers = new ArrayList<>();

  public PluginRegistry() {}

  public PluginRegistry(PluginRegistry defaults) {
    this.connections.addAll(defaults.connections);
    this.clients.addAll(defaults.clients);
    this.servers.addAll(defaults.servers);
  }

  public void addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
    connections.add(interceptor);
  }

  public void addClientPlugin(RSocketInterceptor interceptor) {
    clients.add(interceptor);
  }

  public void addServerPlugin(RSocketInterceptor interceptor) {
    servers.add(interceptor);
  }

  public RSocket applyClient(RSocket rSocket) {
    for (RSocketInterceptor i : clients) {
      rSocket = i.apply(rSocket);
    }

    return rSocket;
  }

  public RSocket applyServer(RSocket rSocket) {
    for (RSocketInterceptor i : servers) {
      rSocket = i.apply(rSocket);
    }

    return rSocket;
  }

  public DuplexConnection applyConnection(
      DuplexConnectionInterceptor.Type type, DuplexConnection connection) {
    for (DuplexConnectionInterceptor i : connections) {
      connection = i.apply(type, connection);
    }

    return connection;
  }
}
