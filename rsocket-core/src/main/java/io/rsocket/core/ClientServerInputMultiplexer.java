/*
 * Copyright 2015-2020 the original author or authors.
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
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameUtil;
import io.rsocket.plugins.DuplexConnectionInterceptor.Type;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

/**
 * {@link DuplexConnection#receive()} is a single stream on which the following type of frames
 * arrive:
 *
 * <ul>
 *   <li>Frames for streams initiated by the initiator of the connection (client).
 *   <li>Frames for streams initiated by the acceptor of the connection (server).
 * </ul>
 *
 * <p>The only way to differentiate these two frames is determining whether the stream Id is odd or
 * even. Even IDs are for the streams initiated by server and odds are for streams initiated by the
 * client.
 */
class ClientServerInputMultiplexer implements CoreSubscriber<ByteBuf>, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.FrameLogger");
  private static final InitializingInterceptorRegistry emptyInterceptorRegistry =
      new InitializingInterceptorRegistry();

  private final InternalDuplexConnection setupReceiver;
  private final InternalDuplexConnection serverReceiver;
  private final InternalDuplexConnection clientReceiver;
  private final DuplexConnection setupConnection;
  private final DuplexConnection serverConnection;
  private final DuplexConnection clientConnection;
  private final DuplexConnection source;
  private final boolean isClient;

  private Subscription s;
  private boolean setupReceived;

  private Throwable t;

  private volatile int state;
  private static final AtomicIntegerFieldUpdater<ClientServerInputMultiplexer> STATE =
      AtomicIntegerFieldUpdater.newUpdater(ClientServerInputMultiplexer.class, "state");

  public ClientServerInputMultiplexer(DuplexConnection source) {
    this(source, emptyInterceptorRegistry, false);
  }

  public ClientServerInputMultiplexer(
      DuplexConnection source, InitializingInterceptorRegistry registry, boolean isClient) {
    this.source = source;
    this.isClient = isClient;
    source = registry.initConnection(Type.SOURCE, source);

    if (!isClient) {
      setupReceiver = new InternalDuplexConnection(this, source);
      setupConnection = registry.initConnection(Type.SETUP, setupReceiver);
    } else {
      setupReceiver = null;
      setupConnection = null;
    }
    serverReceiver = new InternalDuplexConnection(this, source);
    clientReceiver = new InternalDuplexConnection(this, source);
    serverConnection = registry.initConnection(Type.SERVER, serverReceiver);
    clientConnection = registry.initConnection(Type.CLIENT, clientReceiver);
  }

  public DuplexConnection asClientServerConnection() {
    return source;
  }

  public DuplexConnection asServerConnection() {
    return serverConnection;
  }

  public DuplexConnection asClientConnection() {
    return clientConnection;
  }

  public DuplexConnection asSetupConnection() {
    return setupConnection;
  }

  @Override
  public void dispose() {
    source.dispose();
  }

  @Override
  public boolean isDisposed() {
    return source.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.validate(this.s, s)) {
      this.s = s;
      if (isClient) {
        s.request(Long.MAX_VALUE);
      } else {
        // request first SetupFrame
        s.request(1);
      }
    }
  }

  @Override
  public void onNext(ByteBuf frame) {
    int streamId = FrameHeaderCodec.streamId(frame);
    final Type type;
    if (streamId == 0) {
      switch (FrameHeaderCodec.frameType(frame)) {
        case SETUP:
        case RESUME:
        case RESUME_OK:
          type = Type.SETUP;
          setupReceived = true;
          break;
        case LEASE:
        case KEEPALIVE:
        case ERROR:
          type = isClient ? Type.CLIENT : Type.SERVER;
          break;
        default:
          type = isClient ? Type.SERVER : Type.CLIENT;
      }
    } else if ((streamId & 0b1) == 0) {
      type = Type.SERVER;
    } else {
      type = Type.CLIENT;
    }
    if (!isClient && type != Type.SETUP && !setupReceived) {
      final IllegalStateException error =
          new IllegalStateException("SETUP or LEASE frame must be received before any others.");
      this.s.cancel();
      onError(error);
    }

    switch (type) {
      case SETUP:
        final InternalDuplexConnection setupReceiver = this.setupReceiver;
        setupReceiver.onNext(frame);
        setupReceiver.onComplete();
        break;
      case CLIENT:
        clientReceiver.onNext(frame);
        break;
      case SERVER:
        serverReceiver.onNext(frame);
        break;
    }
  }

  @Override
  public void onComplete() {
    final int previousState = STATE.getAndSet(this, Integer.MIN_VALUE);
    if (previousState == Integer.MIN_VALUE || previousState == 0) {
      return;
    }

    if (!isClient) {
      if (!setupReceived) {
        setupReceiver.onComplete();
      }

      if (previousState == 1) {
        return;
      }
    }

    if (clientReceiver.isSubscribed()) {
      clientReceiver.onComplete();
    }
    if (serverReceiver.isSubscribed()) {
      serverReceiver.onComplete();
    }
  }

  @Override
  public void onError(Throwable t) {
    this.t = t;

    final int previousState = STATE.getAndSet(this, Integer.MIN_VALUE);
    if (previousState == Integer.MIN_VALUE || previousState == 0) {
      return;
    }

    if (!isClient) {
      if (!setupReceived) {
        setupReceiver.onError(t);
      }

      if (previousState == 1) {
        return;
      }
    }

    if (clientReceiver.isSubscribed()) {
      clientReceiver.onError(t);
    }
    if (serverReceiver.isSubscribed()) {
      serverReceiver.onError(t);
    }
  }

  boolean notifyRequested() {
    final int currentState = incrementAndGetCheckingState();
    if (currentState == Integer.MIN_VALUE) {
      return false;
    }

    if (isClient) {
      if (currentState == 2) {
        source.receive().subscribe(this);
      }
    } else {
      if (currentState == 1) {
        source.receive().subscribe(this);
      } else if (currentState == 3) {
        // means setup was consumed and we got request from client and server multiplexers
        s.request(Long.MAX_VALUE);
      }
    }

    return true;
  }

  int incrementAndGetCheckingState() {
    int prev, next;
    for (; ; ) {
      prev = this.state;

      if (prev == Integer.MIN_VALUE) {
        return prev;
      }

      next = prev + 1;
      if (STATE.compareAndSet(this, prev, next)) {
        return next;
      }
    }
  }

  private static class InternalDuplexConnection extends Flux<ByteBuf>
      implements Subscription, DuplexConnection {
    private final ClientServerInputMultiplexer clientServerInputMultiplexer;
    private final DuplexConnection source;
    private final boolean debugEnabled;

    private volatile int state;
    static final AtomicIntegerFieldUpdater<InternalDuplexConnection> STATE =
        AtomicIntegerFieldUpdater.newUpdater(InternalDuplexConnection.class, "state");

    CoreSubscriber<? super ByteBuf> actual;

    public InternalDuplexConnection(
        ClientServerInputMultiplexer clientServerInputMultiplexer, DuplexConnection source) {
      this.clientServerInputMultiplexer = clientServerInputMultiplexer;
      this.source = source;
      this.debugEnabled = LOGGER.isDebugEnabled();
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
      if (this.state == 0 && STATE.compareAndSet(this, 0, 1)) {
        this.actual = actual;
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual,
            new IllegalStateException("InternalDuplexConnection allows only single subscription"));
      }
    }

    @Override
    public void request(long n) {
      if (this.state == 1 && STATE.compareAndSet(this, 1, 2)) {
        final ClientServerInputMultiplexer multiplexer = clientServerInputMultiplexer;
        if (!multiplexer.notifyRequested()) {
          final Throwable t = multiplexer.t;
          if (t != null) {
            this.actual.onError(t);
          } else {
            this.actual.onComplete();
          }
        }
      }
    }

    @Override
    public void cancel() {
      // no ops
    }

    void onNext(ByteBuf frame) {
      this.actual.onNext(frame);
    }

    void onComplete() {
      this.actual.onComplete();
    }

    void onError(Throwable t) {
      this.actual.onError(t);
    }

    @Override
    public Mono<Void> send(Publisher<ByteBuf> frame) {
      if (debugEnabled) {
        return Flux.from(frame)
            .doOnNext(f -> LOGGER.debug("sending -> " + FrameUtil.toString(f)))
            .as(source::send);
      }

      return source.send(frame);
    }

    @Override
    public Mono<Void> sendOne(ByteBuf frame) {
      if (debugEnabled) {
        LOGGER.debug("sending -> " + FrameUtil.toString(frame));
      }

      return source.sendOne(frame);
    }

    @Override
    public Flux<ByteBuf> receive() {
      if (debugEnabled) {
        return this.doOnNext(frame -> LOGGER.debug("receiving -> " + FrameUtil.toString(frame)));
      } else {
        return this;
      }
    }

    @Override
    public ByteBufAllocator alloc() {
      return source.alloc();
    }

    @Override
    public void dispose() {
      source.dispose();
    }

    @Override
    public boolean isDisposed() {
      return source.isDisposed();
    }

    public boolean isSubscribed() {
      return this.state != 0;
    }

    @Override
    public Mono<Void> onClose() {
      return source.onClose();
    }

    @Override
    public double availability() {
      return source.availability();
    }
  }
}
