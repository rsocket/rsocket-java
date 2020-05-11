/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocket;
import io.rsocket.TestScheduler;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.DefaultPayload;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.util.RaceTestUtils;

class RSocketRequesterSubscribersTest {

  private static final Set<FrameType> REQUEST_TYPES =
      new HashSet<>(
          Arrays.asList(
              FrameType.METADATA_PUSH,
              FrameType.REQUEST_FNF,
              FrameType.REQUEST_RESPONSE,
              FrameType.REQUEST_STREAM,
              FrameType.REQUEST_CHANNEL));

  private LeaksTrackingByteBufAllocator allocator;
  private RSocket rSocketRequester;
  private TestDuplexConnection connection;

  @BeforeEach
  void setUp() {
    allocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    connection = new TestDuplexConnection(allocator);
    rSocketRequester =
        new RSocketRequester(
            connection,
            PayloadDecoder.DEFAULT,
            StreamIdSupplier.clientSupplier(),
            0,
            0,
            0,
            null,
            RequesterLeaseHandler.None,
            TestScheduler.INSTANCE);
  }

  @ParameterizedTest
  @MethodSource("allInteractions")
  void singleSubscriber(Function<RSocket, Publisher<?>> interaction) {
    Flux<?> response = Flux.from(interaction.apply(rSocketRequester));

    AssertSubscriber assertSubscriberA = AssertSubscriber.create();
    AssertSubscriber assertSubscriberB = AssertSubscriber.create();

    response.subscribe(assertSubscriberA);
    response.subscribe(assertSubscriberB);

    connection.addToReceivedBuffer(PayloadFrameCodec.encodeComplete(connection.alloc(), 1));

    assertSubscriberA.assertTerminated();
    assertSubscriberB.assertTerminated();

    Assertions.assertThat(requestFramesCount(connection.getSent())).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("allInteractions")
  void singleSubscriberInCaseOfRacing(Function<RSocket, Publisher<?>> interaction) {
    for (int i = 1; i < 20000; i += 2) {
      Flux<?> response = Flux.from(interaction.apply(rSocketRequester));
      AssertSubscriber assertSubscriberA = AssertSubscriber.create();
      AssertSubscriber assertSubscriberB = AssertSubscriber.create();

      RaceTestUtils.race(
          () -> response.subscribe(assertSubscriberA), () -> response.subscribe(assertSubscriberB));

      connection.addToReceivedBuffer(PayloadFrameCodec.encodeComplete(connection.alloc(), i));

      assertSubscriberA.assertTerminated();
      assertSubscriberB.assertTerminated();

      Assertions.assertThat(new AssertSubscriber[] {assertSubscriberA, assertSubscriberB})
          .anySatisfy(as -> as.assertError(IllegalStateException.class));

      Assertions.assertThat(connection.getSent())
          .hasSize(1)
          .first()
          .matches(bb -> REQUEST_TYPES.contains(FrameHeaderCodec.frameType(bb)));
      connection.clearSendReceiveBuffers();
    }
  }

  @ParameterizedTest
  @MethodSource("allInteractions")
  void singleSubscriberInteractionsAreLazy(Function<RSocket, Publisher<?>> interaction) {
    Flux<?> response = Flux.from(interaction.apply(rSocketRequester));

    Assertions.assertThat(connection.getSent().size()).isEqualTo(0);
  }

  static long requestFramesCount(Collection<ByteBuf> frames) {
    return frames
        .stream()
        .filter(frame -> REQUEST_TYPES.contains(FrameHeaderCodec.frameType(frame)))
        .count();
  }

  static Stream<Function<RSocket, Publisher<?>>> allInteractions() {
    return Stream.of(
        rSocket -> rSocket.fireAndForget(DefaultPayload.create("test")),
        rSocket -> rSocket.requestResponse(DefaultPayload.create("test")),
        rSocket -> rSocket.requestStream(DefaultPayload.create("test")),
        //        rSocket -> rSocket.requestChannel(Mono.just(DefaultPayload.create("test"))),
        rSocket -> rSocket.metadataPush(DefaultPayload.create("", "test")));
  }
}
