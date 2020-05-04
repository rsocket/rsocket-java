package io.rsocket.core;

import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestServerTransport;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class RSocketServerFragmentationTest {

  @Test
  public void serverErrorsWithEnabledFragmentationOnInsufficientMtu() {
    Assertions.assertThatIllegalArgumentException()
        .isThrownBy(() -> RSocketServer.create().fragment(2))
        .withMessage("The smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @Test
  public void serverSucceedsWithEnabledFragmentationOnSufficientMtu() {
    RSocketServer.create().fragment(100).bind(new TestServerTransport()).block();
  }

  @Test
  public void serverSucceedsWithDisabledFragmentation() {
    RSocketServer.create().bind(new TestServerTransport()).block();
  }

  @Test
  public void clientErrorsWithEnabledFragmentationOnInsufficientMtu() {
    Assertions.assertThatIllegalArgumentException()
        .isThrownBy(() -> RSocketConnector.create().fragment(2))
        .withMessage("The smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @Test
  public void clientSucceedsWithEnabledFragmentationOnSufficientMtu() {
    RSocketConnector.create().fragment(100).connect(new TestClientTransport()).block();
  }

  @Test
  public void clientSucceedsWithDisabledFragmentation() {
    RSocketConnector.connectWith(new TestClientTransport()).block();
  }
}
