package io.rsocket.plugins;

/** JVM wide plugins for RSocket */
public class Plugins {
  private static PluginRegistry DEFAULT = new PluginRegistry();

  private Plugins() {}

  public static void interceptConnection(DuplexConnectionInterceptor interceptor) {
    DEFAULT.addConnectionPlugin(interceptor);
  }

  public static void interceptClient(RSocketInterceptor interceptor) {
    DEFAULT.addClientPlugin(interceptor);
  }

  public static void interceptServer(RSocketInterceptor interceptor) {
    DEFAULT.addServerPlugin(interceptor);
  }

  public static PluginRegistry defaultPlugins() {
    return DEFAULT;
  }
}
