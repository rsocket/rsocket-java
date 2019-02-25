package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.resume.ResumeStateHolder;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.KeepAliveData;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeepAliveTest {
  private static final int TICK_PERIOD = 100;
  private static final int TIMEOUT = 700;

  private TestDuplexConnection testConnection;
  private Function<ByteBuf, Mono<KeepAliveData>> timingsProvider;
  private List<Throwable> errors;
  private Consumer<Throwable> errorConsumer;
  private KeepAliveConnection clientConnection;
  private KeepAliveConnection serverConnection;
  private ByteBufAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = ByteBufAllocator.DEFAULT;
    testConnection = new TestDuplexConnection();

    timingsProvider = f -> Mono.just(new KeepAliveData(TICK_PERIOD, TIMEOUT));
    errors = new ArrayList<>();
    errorConsumer = errors::add;

    clientConnection = KeepAliveConnection.ofClient(allocator, testConnection, timingsProvider, errorConsumer);
    serverConnection = KeepAliveConnection.ofServer(allocator, testConnection, timingsProvider, errorConsumer);
  }

  @Test
  void clientNoFramesBeforeSetup() {

    Mono.delay(Duration.ofSeconds(1)).block();

    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(clientConnection.isDisposed()).isFalse();
    Assertions.assertThat(clientConnection.availability()).isEqualTo(testConnection.availability());
    Assertions.assertThat(testConnection.getSent()).isEmpty();
  }

  @Test
  void clientFramesAfterSetup() {
    clientConnection.sendOne(setupFrame()).subscribe();

    Mono.delay(Duration.ofMillis(500)).block();

    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(clientConnection.isDisposed()).isFalse();
    Collection<ByteBuf> sent = testConnection.getSent();
    Collection<ByteBuf> sentAfterSetup =
        sent.stream()
            .filter(f -> frameType(f) != FrameType.SETUP)
            .collect(Collectors.toList());

    Assertions.assertThat(sentAfterSetup).isNotEmpty();
    sentAfterSetup.forEach(
        f -> {
          Assertions.assertThat(frameType(f)).isEqualTo(FrameType.KEEPALIVE);
          Assertions.assertThat(KeepAliveFrameFlyweight.respondFlag(f)).isEqualTo(true);
          Assertions.assertThat(KeepAliveFrameFlyweight.lastPosition(f)).isEqualTo(0);
        });

    sent.forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void clientCloseOnMissingKeepalives() {
    clientConnection.sendOne(setupFrame()).subscribe();

    Mono.delay(Duration.ofSeconds(1)).block();

    Assertions.assertThat(errors).hasSize(1);
    Throwable err = errors.get(0);
    Assertions.assertThat(err).isExactlyInstanceOf(ConnectionErrorException.class);
    Assertions.assertThat(err.getMessage()).isEqualTo("No keep-alive acks for 700 ms");
    Assertions.assertThat(clientConnection.isDisposed()).isTrue();

    testConnection.getSent().forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void clientResumptionState() {
    ReplayProcessor<Long> resumePositions = ReplayProcessor.create();
    clientConnection.receiveResumePositions(new TestResumeStateHolder()).subscribe(resumePositions);

    clientConnection.sendOne(setupFrame()).subscribe();
    clientConnection.receive().subscribe();

    testConnection.addToReceivedBuffer(keepAliveFrame(false, 1));
    testConnection.addToReceivedBuffer(keepAliveFrame(false, 2));
    testConnection.addToReceivedBuffer(keepAliveFrame(false, 3));

    Mono.delay(Duration.ofMillis(500)).block();

    List<Long> receivedPositions =
        resumePositions.take(3).timeout(Duration.ofMillis(100)).collectList().block();

    Collection<ByteBuf> sent = testConnection.getSent();
    List<Long> sentPositions =
        sent.stream()
            .filter(f -> frameType(f) != FrameType.SETUP)
            .map(KeepAliveFrameFlyweight::lastPosition)
            .limit(4)
            .collect(Collectors.toList());

    Assertions.assertThat(sentPositions).isEqualTo(Arrays.asList(1L, 5L, 6L, 8L));
    Assertions.assertThat(receivedPositions).isEqualTo(Arrays.asList(1L, 2L, 3L));

    sent.forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void serverFrames() {
    serverConnection.sendOne(setupFrame()).subscribe();
    serverConnection.receive().subscribe();
    testConnection.addToReceivedBuffer(keepAliveFrame(true, 0));

    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(clientConnection.isDisposed()).isFalse();

    Collection<ByteBuf> sent = testConnection.getSent();
    Collection<ByteBuf> sentAfterSetup =
        sent
            .stream()
            .filter(f -> frameType(f) != FrameType.SETUP)
            .collect(Collectors.toList());
    Assertions.assertThat(sentAfterSetup).isNotEmpty();

    sentAfterSetup.forEach(
        f -> {
          Assertions.assertThat(frameType(f)).isEqualTo(FrameType.KEEPALIVE);
          Assertions.assertThat(KeepAliveFrameFlyweight.respondFlag(f)).isEqualTo(false);
          Assertions.assertThat(KeepAliveFrameFlyweight.lastPosition(f)).isEqualTo(0);
        });

    sent.forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void serverCloseOnMissingKeepalives() {
    serverConnection.sendOne(setupFrame()).subscribe();

    Mono.delay(Duration.ofSeconds(1)).block();

    Assertions.assertThat(errors).hasSize(1);
    Throwable err = errors.get(0);
    Assertions.assertThat(err).isExactlyInstanceOf(ConnectionErrorException.class);
    Assertions.assertThat(err.getMessage()).isEqualTo("No keep-alive acks for 700 ms");
    Assertions.assertThat(clientConnection.isDisposed()).isTrue();

    testConnection.getSent().forEach(ReferenceCountUtil::safeRelease);
  }

  private ByteBuf keepAliveFrame(boolean respond, int pos) {
    return KeepAliveFrameFlyweight.encode(allocator, respond, pos, Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf setupFrame() {
    return SetupFrameFlyweight.encode(
        allocator,
        false,
        false,
         TICK_PERIOD, TIMEOUT,
        "metadataType",
        "dataType",byteBuf("metadata"), byteBuf("data"));
  }

  private ByteBuf byteBuf(String msg) {
    return Unpooled.wrappedBuffer(msg.getBytes(StandardCharsets.UTF_8));
  }

  private static FrameType frameType(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame);
  }

  private static class TestResumeStateHolder implements ResumeStateHolder {
    private List<Integer> positions = Arrays.asList(1, 5, 6, 8);
    private int counter = 0;

    @Override
    public long impliedPosition() {
      Integer res = positions.get(counter);
      counter = Math.min(counter + 1, positions.size() - 1);
      return res;
    }
  }
}
