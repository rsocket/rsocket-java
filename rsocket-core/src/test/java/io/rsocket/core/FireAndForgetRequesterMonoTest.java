package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA;
import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.core.TestRequesterResponderSupport.genericPayload;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.FrameType;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class FireAndForgetRequesterMonoTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /**
   * General StateMachine transition test. No Fragmentation enabled In this test we check that the
   * given instance of FireAndForgetMono subscribes, and then sends frame immediately
   */
  @ParameterizedTest
  @MethodSource("frameSent")
  public void frameShouldBeSentOnSubscription(Consumer<FireAndForgetRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final Payload payload = genericPayload(activeStreams.getAllocator());
    final FireAndForgetRequesterMono fireAndForgetRequesterMono =
        new FireAndForgetRequesterMono(payload, activeStreams);
    final StateAssert<FireAndForgetRequesterMono> stateAssert =
        StateAssert.assertThat(FireAndForgetRequesterMono.STATE, fireAndForgetRequesterMono);

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(fireAndForgetRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();
    // should not add anything to map
    stateAssert.isTerminated();
    activeStreams.assertNoActiveStreams();

    final ByteBuf frame = activeStreams.getDuplexConnection().awaitFrame();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .typeOf(FrameType.REQUEST_FNF)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();
    activeStreams.getAllocator().assertHasNoLeaks();
  }

  /**
   * General StateMachine transition test. Fragmentation enabled In this test we check that the
   * given instance of FireAndForgetMono subscribes, and then sends all fragments as a separate
   * frame immediately
   */
  @ParameterizedTest
  @MethodSource("frameSent")
  public void frameFragmentsShouldBeSentOnSubscription(
      Consumer<FireAndForgetRequesterMono> monoConsumer) {
    final int mtu = 64;
    final TestRequesterResponderSupport streamManager = TestRequesterResponderSupport.client(mtu);
    final LeaksTrackingByteBufAllocator allocator = streamManager.getAllocator();
    final TestDuplexConnection sender = streamManager.getDuplexConnection();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final FireAndForgetRequesterMono fireAndForgetRequesterMono =
        new FireAndForgetRequesterMono(payload, streamManager);
    final StateAssert<FireAndForgetRequesterMono> stateAssert =
        StateAssert.assertThat(FireAndForgetRequesterMono.STATE, fireAndForgetRequesterMono);

    stateAssert.isUnsubscribed();
    streamManager.assertNoActiveStreams();

    monoConsumer.accept(fireAndForgetRequesterMono);

    // should not add anything to map
    streamManager.assertNoActiveStreams();
    stateAssert.isTerminated();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET_WITH_METADATA) // 64 - 6 (frame headers) - 3 (encoded metadata
        // length) - 3 frame length
        .hasMetadata(Arrays.copyOf(metadata, 52))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_FNF)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment2 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET_WITH_METADATA) // 64 - 6 (frame headers) - 3 (encoded metadata
        // length) - 3 frame length
        .hasMetadata(Arrays.copyOfRange(metadata, 52, 65))
        .hasData(Arrays.copyOf(data, 39))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment3 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET) // 64 - 6 (frame headers) - 3 frame length (no metadata - no length)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 39, 94))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment4 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(35)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 94, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<FireAndForgetRequesterMono>> frameSent() {
    return Stream.of(
        (s) -> StepVerifier.create(s).expectSubscription().expectComplete().verify(),
        FireAndForgetRequesterMono::block);
  }

  /**
   * RefCnt validation test. Should send error if RefCnt is incorrect and frame has already been
   * released Note: ONCE state should be 0
   */
  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<FireAndForgetRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport streamManager = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = streamManager.getAllocator();
    final TestDuplexConnection sender = streamManager.getDuplexConnection();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final FireAndForgetRequesterMono fireAndForgetRequesterMono =
        new FireAndForgetRequesterMono(payload, streamManager);
    final StateAssert<FireAndForgetRequesterMono> stateAssert =
        StateAssert.assertThat(FireAndForgetRequesterMono.STATE, fireAndForgetRequesterMono);

    stateAssert.isUnsubscribed();
    streamManager.assertNoActiveStreams();

    monoConsumer.accept(fireAndForgetRequesterMono);

    stateAssert.isTerminated();
    streamManager.assertNoActiveStreams();

    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<FireAndForgetRequesterMono>>
      shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        fireAndForgetRequesterMono ->
            Assertions.assertThatThrownBy(fireAndForgetRequesterMono::block)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  /**
   * Check that proper payload size validation is enabled so in case payload fragmentation is
   * disabled we will not send anything bigger that 16MB (see specification for MAX frame size)
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<FireAndForgetRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport streamManager = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = streamManager.getAllocator();
    final TestDuplexConnection sender = streamManager.getDuplexConnection();

    final byte[] metadata = new byte[FRAME_LENGTH_MASK];
    final byte[] data = new byte[FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final FireAndForgetRequesterMono fireAndForgetRequesterMono =
        new FireAndForgetRequesterMono(payload, streamManager);
    final StateAssert<FireAndForgetRequesterMono> stateAssert =
        StateAssert.assertThat(FireAndForgetRequesterMono.STATE, fireAndForgetRequesterMono);

    stateAssert.isUnsubscribed();
    streamManager.assertNoActiveStreams();

    monoConsumer.accept(fireAndForgetRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    stateAssert.isTerminated();
    streamManager.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<FireAndForgetRequesterMono>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage(
                                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        fireAndForgetRequesterMono ->
            Assertions.assertThatThrownBy(fireAndForgetRequesterMono::block)
                .hasMessage(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                .isInstanceOf(IllegalArgumentException.class));
  }

  /**
   * Ensures that frame will not be sent if we dont have availability for that. Options: 1. RSocket
   * disposed / Connection Error, so all racing on existing interactions should be terminated as
   * well 2. RSocket tries to use lease and end-ups with no available leases
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<FireAndForgetRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport streamManager =
        TestRequesterResponderSupport.client(new RuntimeException("test"));
    final LeaksTrackingByteBufAllocator allocator = streamManager.getAllocator();
    final TestDuplexConnection sender = streamManager.getDuplexConnection();
    final Payload payload = genericPayload(allocator);

    final FireAndForgetRequesterMono fireAndForgetRequesterMono =
        new FireAndForgetRequesterMono(payload, streamManager);
    final StateAssert<FireAndForgetRequesterMono> stateAssert =
        StateAssert.assertThat(FireAndForgetRequesterMono.STATE, fireAndForgetRequesterMono);

    stateAssert.isUnsubscribed();
    streamManager.assertNoActiveStreams();

    monoConsumer.accept(fireAndForgetRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    stateAssert.isTerminated();
    streamManager.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<FireAndForgetRequesterMono>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        fireAndForgetRequesterMono ->
            Assertions.assertThatThrownBy(fireAndForgetRequesterMono::block)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  /** Ensures single subscription happens in case of racing */
  @Test
  public void shouldSubscribeExactlyOnce1() {
    final TestRequesterResponderSupport streamManager = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = streamManager.getAllocator();
    final TestDuplexConnection sender = streamManager.getDuplexConnection();

    for (int i = 1; i < 50000; i += 2) {
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final FireAndForgetRequesterMono fireAndForgetRequesterMono =
          new FireAndForgetRequesterMono(payload, streamManager);
      final StateAssert<FireAndForgetRequesterMono> stateAssert =
          StateAssert.assertThat(FireAndForgetRequesterMono.STATE, fireAndForgetRequesterMono);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () -> {
                        AtomicReference<Throwable> atomicReference = new AtomicReference();
                        fireAndForgetRequesterMono.subscribe(null, atomicReference::set);
                        Throwable throwable = atomicReference.get();
                        if (throwable != null) {
                          throw Exceptions.propagate(throwable);
                        }
                      },
                      fireAndForgetRequesterMono::block))
          .matches(
              t -> {
                Assertions.assertThat(t)
                    .hasMessageContaining("FireAndForgetMono allows only a single Subscriber");
                return true;
              });

      final ByteBuf frame = sender.awaitFrame();
      FrameAssert.assertThat(frame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_FNF)
          .hasClientSideStreamId()
          .hasStreamId(i)
          .hasNoLeaks();

      stateAssert.isTerminated();
      streamManager.assertNoActiveStreams();
    }

    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  @Test
  public void checkName() {
    final TestRequesterResponderSupport testRequesterResponderSupport =
        TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = testRequesterResponderSupport.getAllocator();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final FireAndForgetRequesterMono fireAndForgetRequesterMono =
        new FireAndForgetRequesterMono(payload, testRequesterResponderSupport);

    Assertions.assertThat(Scannable.from(fireAndForgetRequesterMono).name())
        .isEqualTo("source(FireAndForgetMono)");
    allocator.assertHasNoLeaks();
  }
}
