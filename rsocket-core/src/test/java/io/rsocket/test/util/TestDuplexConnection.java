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

package io.rsocket.test.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;

/**
 * An implementation of {@link DuplexConnection} that provides functionality to modify the behavior
 * dynamically.
 */
public class TestDuplexConnection implements DuplexConnection {

  private static final Logger logger = LoggerFactory.getLogger(TestDuplexConnection.class);

  private final LinkedBlockingQueue<ByteBuf> sent;
  private final DirectProcessor<ByteBuf> sentPublisher;
  private final FluxSink<ByteBuf> sendSink;
  private final DirectProcessor<ByteBuf> received;
  private final FluxSink<ByteBuf> receivedSink;
  private final MonoProcessor<Void> onClose;
  private final ByteBufAllocator allocator;
  private volatile double availability = 1;
  private volatile int initialSendRequestN = Integer.MAX_VALUE;

  public TestDuplexConnection(ByteBufAllocator allocator) {
    this.allocator = allocator;
    this.sent = new LinkedBlockingQueue<>();
    this.received = DirectProcessor.create();
    this.receivedSink = received.sink();
    this.sentPublisher = DirectProcessor.create();
    this.sendSink = sentPublisher.sink();
    this.onClose = MonoProcessor.create();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    if (availability <= 0) {
      return Mono.error(
          new IllegalStateException("RSocket not available. Availability: " + availability));
    }
    return Flux.from(frames)
        .doOnNext(
            frame -> {
              sendSink.next(frame);
              sent.offer(frame);
              System.out.println("here : " + sent.toString());
            })
        .doOnError(throwable -> logger.error("Error in send stream on test connection.", throwable))
        .then();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return received.transform(
        Operators.<ByteBuf, ByteBuf>lift(
            (__, actual) ->
                new CoreSubscriber<ByteBuf>() {
                  @Override
                  public void onSubscribe(Subscription s) {
                    actual.onSubscribe(s);
                  }

                  @Override
                  public void onNext(ByteBuf byteBuf) {
                    try {
                      actual.onNext(byteBuf);
                    } finally {
                      byteBuf.release();
                    }
                  }

                  @Override
                  public void onError(Throwable t) {
                    actual.onError(t);
                  }

                  @Override
                  public void onComplete() {
                    actual.onComplete();
                  }
                }));
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
  }

  @Override
  public double availability() {
    return availability;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  public ByteBuf awaitSend() throws InterruptedException {
    return sent.take();
  }

  public void setAvailability(double availability) {
    this.availability = availability;
  }

  public BlockingQueue<ByteBuf> getSent() {
    return sent;
  }

  public Publisher<ByteBuf> getSentAsPublisher() {
    return sentPublisher;
  }

  public void addToReceivedBuffer(ByteBuf... received) {
    for (ByteBuf frame : received) {
      this.receivedSink.next(frame);
    }
  }

  public void clearSendReceiveBuffers() {
    sent.clear();
  }

  public void setInitialSendRequestN(int initialSendRequestN) {
    this.initialSendRequestN = initialSendRequestN;
  }
}
