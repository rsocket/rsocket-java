package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.transport.ServerTransport.ConnectionAcceptor;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.*;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

public class SetupRejectionTest {

  @Test
  void responderRejectSetup() {
    SingleConnectionTransport transport = new SingleConnectionTransport();

    String errorMsg = "error";
    RejectingAcceptor acceptor = new RejectingAcceptor(errorMsg);
    RSocketServer.create().acceptor(acceptor).bind(transport).block();

    transport.connect();

    ByteBuf sentFrame = transport.awaitSent();
    assertThat(FrameHeaderCodec.frameType(sentFrame)).isEqualTo(FrameType.ERROR);
    RuntimeException error = Exceptions.from(0, sentFrame);
    assertThat(errorMsg).isEqualTo(error.getMessage());
    assertThat(error).isInstanceOf(RejectedSetupException.class);
    RSocket acceptorSender = acceptor.senderRSocket().block();
    assertThat(acceptorSender.isDisposed()).isTrue();
  }

  @Test
  @Disabled("FIXME: needs to be revised")
  void requesterStreamsTerminatedOnZeroErrorFrame() {
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    TestDuplexConnection conn = new TestDuplexConnection(allocator);
    RSocketRequester rSocket =
        new RSocketRequester(
            conn,
            DefaultPayload::create,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            0,
            0,
            null,
            RequesterLeaseHandler.None);

    String errorMsg = "error";

    StepVerifier.create(
            rSocket
                .requestResponse(DefaultPayload.create("test"))
                .doOnRequest(
                    ignored ->
                        conn.addToReceivedBuffer(
                            ErrorFrameCodec.encode(
                                ByteBufAllocator.DEFAULT,
                                0,
                                new RejectedSetupException(errorMsg)))))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && errorMsg.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    assertThat(rSocket.isDisposed()).isTrue();
  }

  @Test
  void requesterNewStreamsTerminatedAfterZeroErrorFrame() {
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    TestDuplexConnection conn = new TestDuplexConnection(allocator);
    RSocketRequester rSocket =
        new RSocketRequester(
            conn,
            DefaultPayload::create,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            0,
            0,
            null,
            RequesterLeaseHandler.None);

    conn.addToReceivedBuffer(
        ErrorFrameCodec.encode(ByteBufAllocator.DEFAULT, 0, new RejectedSetupException("error")));

    StepVerifier.create(
            rSocket
                .requestResponse(DefaultPayload.create("test"))
                .delaySubscription(Duration.ofMillis(100)))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && "error".equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));
  }

  private static class RejectingAcceptor implements SocketAcceptor {
    private final String errorMessage;
    private final UnicastProcessor<RSocket> senderRSockets = UnicastProcessor.create();

    public RejectingAcceptor(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      senderRSockets.onNext(sendingSocket);
      return Mono.error(new RuntimeException(errorMessage));
    }

    public Mono<RSocket> senderRSocket() {
      return senderRSockets.next();
    }
  }

  private static class SingleConnectionTransport implements ServerTransport<TestCloseable> {

    private final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    private final TestDuplexConnection conn = new TestDuplexConnection(allocator);

    @Override
    public Mono<TestCloseable> start(ConnectionAcceptor acceptor) {
      return Mono.just(new TestCloseable(acceptor, conn));
    }

    public ByteBuf awaitSent() {
      return conn.awaitFrame();
    }

    public void connect() {
      Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
      ByteBuf setup = SetupFrameCodec.encode(allocator, false, 0, 42, "mdMime", "dMime", payload);

      conn.addToReceivedBuffer(setup);
    }
  }

  private static class TestCloseable implements Closeable {

    private final DuplexConnection conn;

    TestCloseable(ConnectionAcceptor acceptor, DuplexConnection conn) {
      this.conn = conn;
      Mono.from(acceptor.apply(conn)).subscribe(notUsed -> {}, err -> conn.dispose());
    }

    @Override
    public Mono<Void> onClose() {
      return conn.onClose();
    }

    @Override
    public void dispose() {
      conn.dispose();
    }
  }
}
