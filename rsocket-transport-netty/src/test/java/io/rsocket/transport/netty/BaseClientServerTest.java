package io.rsocket.transport.netty;

import io.rsocket.test.ClientSetupRule;
import org.junit.Rule;
import org.junit.Test;

public abstract class BaseClientServerTest<T extends ClientSetupRule<?, ?>> {
  @Rule public final T setup = createClientServer();

  protected abstract T createClientServer();

  @Test(timeout = 10000)
  public void testFireNForget10() {
    setup.testFireAndForget(10);
  }

  @Test(timeout = 10000)
  public void testPushMetadata10() {
    setup.testMetadata(10);
  }

  @Test(timeout = 10000)
  public void testRequestResponse1() {
    setup.testRequestResponseN(1);
  }

  @Test(timeout = 10000)
  public void testRequestResponse10() {
    setup.testRequestResponseN(10);
  }

  @Test(timeout = 10000)
  public void testRequestResponse100() {
    setup.testRequestResponseN(100);
  }

  @Test(timeout = 10000)
  public void testRequestResponse10_000() {
    setup.testRequestResponseN(10_000);
  }

  @Test(timeout = 10000)
  public void testRequestStream() {
    setup.testRequestStream();
  }

  @Test(timeout = 10000)
  public void testRequestStreamWithRequestN() {
    setup.testRequestStreamWithRequestN();
  }
}
