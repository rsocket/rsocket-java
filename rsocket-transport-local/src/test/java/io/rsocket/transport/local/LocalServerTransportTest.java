package io.rsocket.transport.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

public class LocalServerTransportTest {
  @Test
  public void testEphemeral() {
    LocalServerTransport st1 = LocalServerTransport.createEphemeral();
    LocalServerTransport st2 = LocalServerTransport.createEphemeral();
    assertNotEquals(st2.getName(), st1.getName());
  }

  @Test
  public void testNamed() {
    LocalServerTransport st = LocalServerTransport.create("LocalServerTransportTest");
    assertEquals("LocalServerTransportTest", st.getName());
  }
}
