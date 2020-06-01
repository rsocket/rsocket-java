package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

public class RSocketConnectorTest {

  @Test
  public void ensuresThatSetupPayloadCanBeRetained() {
    MonoProcessor<ConnectionSetupPayload> retainedSetupPayload = MonoProcessor.create();
    TestClientTransport transport = new TestClientTransport();

    ByteBuf data = transport.alloc().buffer();

    data.writeCharSequence("data", CharsetUtil.UTF_8);

    RSocketConnector.create()
        .setupPayload(ByteBufPayload.create(data))
        .acceptor(
            (setup, sendingSocket) -> {
              retainedSetupPayload.onNext(setup.retain());
              return Mono.just(new RSocket() {});
            })
        .connect(transport)
        .block();

    Assertions.assertThat(transport.testConnection().getSent())
        .hasSize(1)
        .first()
        .matches(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return !payload.hasMetadata() && payload.getDataUtf8().equals("data");
            })
        .matches(buf -> buf.refCnt() == 2)
        .matches(
            buf -> {
              buf.release();
              return buf.refCnt() == 1;
            });

    retainedSetupPayload
        .as(StepVerifier::create)
        .expectNextMatches(
            setup -> {
              String dataUtf8 = setup.getDataUtf8();
              return "data".equals(dataUtf8) && setup.release();
            })
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(retainedSetupPayload.peek().refCnt()).isZero();

    transport.alloc().assertHasNoLeaks();
  }

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
