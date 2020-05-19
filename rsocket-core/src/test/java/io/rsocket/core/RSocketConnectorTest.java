package io.rsocket.core;

import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketConnectorTest {

  @Test
  public void ensuresThatMonoFromRSocketConnectorCanBeUsedForMultipleSubscriptions() {
    Payload setupPayload = ByteBufPayload.create("TestData", "TestMetadata");

    Assertions.assertThat(setupPayload.refCnt()).isOne();

    TestClientTransport testClientTransport = new TestClientTransport();
    Mono<RSocket> connectionMono =
        RSocketConnector.create().setupPayload(setupPayload).connect(testClientTransport);

    connectionMono
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    connectionMono
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    Assertions.assertThat(testClientTransport.testConnection().getSent())
        .hasSize(2)
        .allMatch(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return payload.getDataUtf8().equals("TestData")
                  && payload.getMetadataUtf8().equals("TestMetadata");
            })
        .allMatch(ReferenceCounted::release);
    Assertions.assertThat(setupPayload.refCnt()).isZero();
  }
}
