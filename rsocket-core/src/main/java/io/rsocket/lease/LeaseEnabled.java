package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.plugins.PluginRegistry;

public class LeaseEnabled {
  private final PluginRegistry pluginRegistry;
  private final DuplexConnection clientConnection;

  LeaseEnabled(PluginRegistry pluginRegistry, DuplexConnection clientConnection) {
    this.pluginRegistry = pluginRegistry;
    this.clientConnection = clientConnection;
  }

  public PluginRegistry getInterceptor() {
    return pluginRegistry;
  }

  public DuplexConnection getClientConnection() {
    return clientConnection;
  }
}
