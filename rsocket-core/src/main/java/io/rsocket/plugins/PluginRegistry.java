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
