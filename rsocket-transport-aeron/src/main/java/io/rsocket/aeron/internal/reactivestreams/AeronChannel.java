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

package io.rsocket.aeron.internal.reactivestreams;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.rsocket.aeron.internal.EventLoop;
import java.util.Objects;
import org.agrona.DirectBuffer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** */
public class AeronChannel implements ReactiveStreamsRemote.Channel<DirectBuffer>, Disposable {
  private final String name;
  private final Publication destination;
  private final Subscription source;
  private final AeronOutPublisher outPublisher;
  private final EventLoop eventLoop;

  /**
   * Creates on end of a bi-directional channel
   *
   * @param name name of the channel
   * @param destination {@code Publication} to send data to
   * @param source Aeron {@code Subscription} to listen to data on
   * @param eventLoop {@link EventLoop} used to poll data on
   * @param sessionId sessionId between the {@code Publication} and the remote {@code Subscription}
   */
  public AeronChannel(
      String name,
      Publication destination,
      Subscription source,
      EventLoop eventLoop,
      int sessionId) {
    this.destination = destination;
    this.source = source;
    this.name = name;
    this.eventLoop = eventLoop;
    this.outPublisher = new AeronOutPublisher(name, sessionId, source, eventLoop);
  }

  /**
   * Subscribes to a stream of DirectBuffers and sends the to an Aeron Publisher
   *
   * @param in the publisher of buffers.
   * @return Mono the completes when all publishers have been sent.
   */
  public Mono<Void> send(Flux<? extends DirectBuffer> in) {
    AeronInSubscriber inSubscriber = new AeronInSubscriber(name, destination);
    Objects.requireNonNull(in, "in must not be null");
    return Mono.create(
        sink -> in.doOnComplete(sink::success).doOnError(sink::error).subscribe(inSubscriber));
  }

  /**
   * Returns ReactiveStreamsRemote.Out of DirectBuffer that can only be subscribed to once per
   * channel
   *
   * @return ReactiveStreamsRemote.Out of DirectBuffer
   */
  public Flux<? extends DirectBuffer> receive() {
    return outPublisher;
  }

  @Override
  public void dispose() {
    destination.close();
    source.close();
  }

  @Override
  public boolean isDisposed() {
    return destination.isClosed() && source.isClosed();
  }

  @Override
  public String toString() {
    return "AeronChannel{" + "name='" + name + '\'' + '}';
  }
}
