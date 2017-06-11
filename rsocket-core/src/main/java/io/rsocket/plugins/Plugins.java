package io.rsocket.plugins;

/** JVM wide plugins for RSocket */
public class Plugins {
  private static PluginX DEFAULT = new PluginX();

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

  public static PluginX defaultPlugins() {
    return new PluginX(DEFAULT);
  }
}
