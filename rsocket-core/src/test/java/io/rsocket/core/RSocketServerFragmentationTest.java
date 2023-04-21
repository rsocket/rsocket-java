package io.rsocket.core;

import io.rsocket.Closeable;
import io.rsocket.FrameAssert;
import io.rsocket.frame.FrameType;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestServerTransport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class RSocketServerFragmentationTest {

  @Test
  public void serverErrorsWithEnabledFragmentationOnInsufficientMtu() {
    Assertions.assertThatIllegalArgumentException()
        .isThrownBy(() -> RSocketServer.create().fragment(2))
        .withMessage("The smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @Test
  public void serverSucceedsWithEnabledFragmentationOnSufficientMtu() {
    TestServerTransport transport = new TestServerTransport();
    Closeable closeable = RSocketServer.create().fragment(100).bind(transport).block();
    closeable.dispose();
    transport.alloc().assertHasNoLeaks();
  }

  @Test
  public void serverSucceedsWithDisabledFragmentation() {
    TestServerTransport transport = new TestServerTransport();
    Closeable closeable = RSocketServer.create().bind(transport).block();
    closeable.dispose();
    transport.alloc().assertHasNoLeaks();
  }

  @Test
  public void clientErrorsWithEnabledFragmentationOnInsufficientMtu() {
    Assertions.assertThatIllegalArgumentException()
        .isThrownBy(() -> RSocketConnector.create().fragment(2))
        .withMessage("The smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @Test
  public void clientSucceedsWithEnabledFragmentationOnSufficientMtu() {
    TestClientTransport transport = new TestClientTransport();
    RSocketConnector.create().fragment(100).connect(transport).block();
    FrameAssert.assertThat(transport.testConnection().pollFrame())
        .typeOf(FrameType.SETUP)
        .hasNoLeaks();
    transport.testConnection().dispose();
    transport.alloc().assertHasNoLeaks();
  }

  @Test
  public void clientSucceedsWithDisabledFragmentation() {
    TestClientTransport transport = new TestClientTransport();
    RSocketConnector.connectWith(transport).block();
    FrameAssert.assertThat(transport.testConnection().pollFrame())
        .typeOf(FrameType.SETUP)
        .hasNoLeaks();
    transport.testConnection().dispose();
    transport.alloc().assertHasNoLeaks();
  }
}
