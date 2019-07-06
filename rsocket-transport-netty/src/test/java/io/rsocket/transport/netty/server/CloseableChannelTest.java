/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.transport.netty.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableChannel;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

final class CloseableChannelTest {

  private final Mono<? extends DisposableChannel> channel =
      TcpServer.create().handle((in, out) -> Mono.empty()).bind();

  @DisplayName("returns the address of the context")
  @Test
  void address() {
    channel
        .map(CloseableChannelTest::closeableChannel)
        .map(CloseableChannel::address)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();
  }

  @DisplayName("creates instance")
  @Test
  void constructor() {
    channel
        .map(CloseableChannelTest::closeableChannel)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();
  }

  @DisplayName("constructor throws NullPointerException with null context")
  @Test
  void constructorNullContext() {
    assertThatNullPointerException()
        .isThrownBy(() -> CloseableChannelTest.closeableChannel(null))
        .withMessage("channel must not be null");
  }

  @Disabled(
      "NettyContext isDisposed() is not accurate\n"
          + "https://github.com/reactor/reactor-netty/issues/360")
  @DisplayName("disposes context")
  @Test
  void dispose() {
    channel
        .map(CloseableChannelTest::closeableChannel)
        .delayUntil(
            closeable -> {
              closeable.dispose();
              return closeable.onClose().log();
            })
        .as(StepVerifier::create)
        .assertNext(closeable -> assertThat(closeable.isDisposed()).isTrue())
        .verifyComplete();
  }

  private static CloseableChannel closeableChannel(DisposableChannel channel) {
    return new CloseableChannel(channel, new DefaultChannelGroup(new DefaultEventExecutor()));
  }
}
