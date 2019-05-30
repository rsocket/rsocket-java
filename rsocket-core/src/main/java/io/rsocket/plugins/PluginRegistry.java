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
  private List<RSocketInterceptor> requesters = new ArrayList<>();
  private List<RSocketInterceptor> responders = new ArrayList<>();

  public PluginRegistry() {}

  public PluginRegistry(PluginRegistry defaults) {
    this.connections.addAll(defaults.connections);
    this.requesters.addAll(defaults.requesters);
    this.responders.addAll(defaults.responders);
  }

  public void addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
    connections.add(interceptor);
  }

  /** Deprecated. Use {@link #addRequesterPlugin(RSocketInterceptor)} instead */
  @Deprecated
  public void addClientPlugin(RSocketInterceptor interceptor) {
    addRequesterPlugin(interceptor);
  }

  public void addRequesterPlugin(RSocketInterceptor interceptor) {
    requesters.add(interceptor);
  }

  /** Deprecated. Use {@link #addResponderPlugin(RSocketInterceptor)} instead */
  @Deprecated
  public void addServerPlugin(RSocketInterceptor interceptor) {
    addResponderPlugin(interceptor);
  }

  public void addResponderPlugin(RSocketInterceptor interceptor) {
    responders.add(interceptor);
  }

  /** Deprecated. Use {@link #applyRequester(RSocket)} instead */
  @Deprecated
  public RSocket applyClient(RSocket rSocket) {
    return applyRequester(rSocket);
  }

  public RSocket applyRequester(RSocket rSocket) {
    for (RSocketInterceptor i : requesters) {
      rSocket = i.apply(rSocket);
    }

    return rSocket;
  }

  /** Deprecated. Use {@link #applyResponder(RSocket)} instead */
  @Deprecated
  public RSocket applyServer(RSocket rSocket) {
    return applyResponder(rSocket);
  }

  public RSocket applyResponder(RSocket rSocket) {
    for (RSocketInterceptor i : responders) {
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
