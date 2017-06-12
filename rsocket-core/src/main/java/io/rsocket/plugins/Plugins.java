package io.rsocket.plugins;

/** JVM wide plugins for RSocket */
public class Plugins {
  private static PluginRegistry DEFAULT = new PluginRegistry();

  private Plugins() {}

  public static void registerConnectionPlugin(DuplexConnectionInterceptor interceptor) {
    DEFAULT.addConnectionPlugin(interceptor);
  }

  public static void registerClientPlugin(RSocketInterceptor interceptor) {
    DEFAULT.addClientPlugin(interceptor);
  }

  public static void registerServerPlugin(RSocketInterceptor interceptor) {
    DEFAULT.addServerPlugin(interceptor);
  }

  public static PluginRegistry defaultPlugins() {
    return DEFAULT;
  }
}
