package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    // Keep the data and metadata around so we can try changing them independently
    ByteBuf dataBuf = setupPayload.data();
    ByteBuf metadataBuf = setupPayload.metadata();
    dataBuf.retain();
    metadataBuf.retain();

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

    // Changing the original data and metadata should not impact the SetupPayload
    dataBuf.writerIndex(dataBuf.readerIndex());
    dataBuf.writeChar('d');
    dataBuf.release();

    metadataBuf.writerIndex(metadataBuf.readerIndex());
    metadataBuf.writeChar('m');
    metadataBuf.release();

    Assertions.assertThat(testClientTransport.testConnection().getSent())
        .hasSize(2)
        .allMatch(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return payload.getDataUtf8().equals("TestData")
                  && payload.getMetadataUtf8().equals("TestMetadata");
            })
        .allMatch(
            byteBuf -> {
              System.out.println("calling release " + byteBuf.refCnt());
              return byteBuf.release();
            });
    Assertions.assertThat(setupPayload.refCnt()).isZero();
  }

  @Test
  public void ensuresThatSetupPayloadProvidedAsMonoIsReleased() {
    List<Payload> saved = new ArrayList<>();
    Mono<Payload> setupPayloadMono =
        Mono.create(
            sink -> {
              Payload payload = ByteBufPayload.create("TestData", "TestMetadata");
              saved.add(payload);
              sink.success(payload);
            });

    TestClientTransport testClientTransport = new TestClientTransport();
    Mono<RSocket> connectionMono =
        RSocketConnector.create().setupPayload(setupPayloadMono).connect(testClientTransport);

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

    Assertions.assertThat(saved)
        .as("Metadata and data were consumed and released as slices")
        .allMatch(
            payload ->
                payload.refCnt() == 1
                    && payload.data().refCnt() == 0
                    && payload.metadata().refCnt() == 0);
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeLessThenMtu() {
    RSocketConnector.create()
        .fragment(128)
        .connect(new TestClientTransport().withMaxFrameLength(64))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maximumTransmissionUnit[128] exceeds configured maxFrameLength[64]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPayloadSize() {
    RSocketConnector.create()
        .maxInboundPayloadSize(128)
        .connect(new TestClientTransport().withMaxFrameLength(256))
        .as(StepVerifier::create)
        .expectErrorMessage("Configured maxFrameLength[256] exceeds maxPayloadSize[128]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPossibleFrameLength() {
    RSocketConnector.create()
        .connect(new TestClientTransport().withMaxFrameLength(Integer.MAX_VALUE))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maxFrameLength["
                + Integer.MAX_VALUE
                + "] "
                + "exceeds maxFrameLength limit "
                + FRAME_LENGTH_MASK)
        .verify();
  }
}
