package io.rsocket.transport.netty;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.uri.UriTransportRegistry;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class NettyUriTransportRegistryTest {
    @Test
    public void testTestRegistered() {
        ClientTransport transport = UriTransportRegistry.forUri("tcp://localhost:9898");

        assertTrue(transport instanceof TcpClientTransport);
    }
}
