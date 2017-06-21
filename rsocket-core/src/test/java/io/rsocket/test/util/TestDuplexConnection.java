/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.test.util;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * An implementation of {@link DuplexConnection} that provides functionality to modify the behavior
 * dynamically.
 */
public class TestDuplexConnection implements DuplexConnection {

  private static final Logger logger = LoggerFactory.getLogger(TestDuplexConnection.class);

  private final LinkedBlockingQueue<Frame> sent;
  private final DirectProcessor<Frame> sentPublisher;
  private final DirectProcessor<Frame> received;
  private final MonoProcessor<Void> close;
  private final ConcurrentLinkedQueue<Subscriber<Frame>> sendSubscribers;
  private volatile double availability = 1;
  private volatile int initialSendRequestN = Integer.MAX_VALUE;

  public TestDuplexConnection() {
    sent = new LinkedBlockingQueue<>();
    received = DirectProcessor.create();
    sentPublisher = DirectProcessor.create();
    sendSubscribers = new ConcurrentLinkedQueue<>();
    close = MonoProcessor.create();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    if (availability <= 0) {
      return Mono.error(
          new IllegalStateException("RSocket not available. Availability: " + availability));
    }
    Subscriber<Frame> subscriber = TestSubscriber.create(initialSendRequestN);
    Flux.from(frames)
        .doOnNext(
            frame -> {
              sent.offer(frame);
              sentPublisher.onNext(frame);
            })
        .doOnError(
            throwable -> {
              logger.error("Error in send stream on test connection.", throwable);
            })
        .subscribe(subscriber);
    sendSubscribers.add(subscriber);
    return Mono.empty();
  }

  @Override
  public Flux<Frame> receive() {
    return received;
  }

  @Override
  public double availability() {
    return availability;
  }

  @Override
  public Mono<Void> close() {
    return close;
  }

  @Override
  public Mono<Void> onClose() {
    return close();
  }

  public Frame awaitSend() throws InterruptedException {
    return sent.take();
  }

  public void setAvailability(double availability) {
    this.availability = availability;
  }

  public Collection<Frame> getSent() {
    return sent;
  }

  public Publisher<Frame> getSentAsPublisher() {
    return sentPublisher;
  }

  public void addToReceivedBuffer(Frame... received) {
    for (Frame frame : received) {
      this.received.onNext(frame);
    }
  }

  public void clearSendReceiveBuffers() {
    sent.clear();
    sendSubscribers.clear();
  }

  public void setInitialSendRequestN(int initialSendRequestN) {
    this.initialSendRequestN = initialSendRequestN;
  }

  public Collection<Subscriber<Frame>> getSendSubscribers() {
    return sendSubscribers;
  }
}
