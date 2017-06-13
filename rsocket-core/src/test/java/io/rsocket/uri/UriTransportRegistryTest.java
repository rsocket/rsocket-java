package io.rsocket.uri;

import static org.junit.Assert.assertTrue;

import io.rsocket.DuplexConnection;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.transport.ClientTransport;
import org.junit.Test;

public class UriTransportRegistryTest {
  @Test
  public void testTestRegistered() {
    ClientTransport test = UriTransportRegistry.clientForUri("test://test");

    DuplexConnection duplexConnection = test.connect().block();

    assertTrue(duplexConnection instanceof TestDuplexConnection);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testTestUnregistered() {
    ClientTransport test = UriTransportRegistry.clientForUri("mailto://bonson@baulsupp.net");

    test.connect().block();
  }
}
