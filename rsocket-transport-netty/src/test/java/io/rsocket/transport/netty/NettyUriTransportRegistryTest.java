package io.rsocket.transport.netty;

import static org.junit.Assert.assertTrue;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.uri.UriTransportRegistry;
import org.junit.Test;

public class NettyUriTransportRegistryTest {
  @Test
  public void testTcpClient() {
    ClientTransport transport = UriTransportRegistry.clientForUri("tcp://localhost:9898");

    assertTrue(transport instanceof TcpClientTransport);
  }

  @Test
  public void testTcpServer() {
    ServerTransport transport = UriTransportRegistry.serverForUri("tcp://localhost:9898");

    assertTrue(transport instanceof TcpServerTransport);
  }

  @Test
  public void testWsClient() {
    ClientTransport transport = UriTransportRegistry.clientForUri("ws://localhost:9898");

    assertTrue(transport instanceof WebsocketClientTransport);
  }

  @Test
  public void testWsServer() {
    ServerTransport transport = UriTransportRegistry.serverForUri("ws://localhost:9898");

    assertTrue(transport instanceof WebsocketServerTransport);
  }
}
