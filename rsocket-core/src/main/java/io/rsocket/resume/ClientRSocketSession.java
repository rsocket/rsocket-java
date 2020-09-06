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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.ResumeFrameCodec;
import io.rsocket.frame.ResumeOkFrameCodec;
import io.rsocket.keepalive.KeepAliveSupport;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.function.Tuple2;
import reactor.util.retry.Retry;

public class ClientRSocketSession
    implements RSocketSession,
        ResumeStateHolder,
        CoreSubscriber<Tuple2<ByteBuf, DuplexConnection>> {

  private static final Logger logger = LoggerFactory.getLogger(ClientRSocketSession.class);

  final ResumableDuplexConnection resumableConnection;
  final Mono<Tuple2<ByteBuf, DuplexConnection>> connectionFactory;
  final ResumableFramesStore resumableFramesStore;

  final ByteBufAllocator allocator;
  final Duration resumeSessionDuration;
  final Retry retry;
  final boolean cleanupStoreOnKeepAlive;

  volatile ByteBuf resumeToken;

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<ClientRSocketSession, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(ClientRSocketSession.class, Subscription.class, "s");

  KeepAliveSupport keepAliveSupport;

  public ClientRSocketSession(
      ByteBuf resumeToken,
      DuplexConnection initialDuplexConnection,
      ResumableDuplexConnection resumableDuplexConnection,
      Mono<DuplexConnection> connectionFactory,
      Function<DuplexConnection, Mono<Tuple2<ByteBuf, DuplexConnection>>> connectionTransformer,
      ResumableFramesStore resumableFramesStore,
      Duration resumeSessionDuration,
      Retry retry,
      boolean cleanupStoreOnKeepAlive) {
    this.resumeToken = resumeToken;
    this.connectionFactory =
        connectionFactory.flatMap(
            dc -> {
              dc.sendFrame(
                  0,
                  ResumeFrameCodec.encode(
                      dc.alloc(),
                      resumeToken.retain(),
                      resumableFramesStore.frameImpliedPosition(),
                      resumableFramesStore.framePosition()));

              return connectionTransformer.apply(dc);
            });
    this.resumableFramesStore = resumableFramesStore;
    this.allocator = resumableDuplexConnection.alloc();
    this.resumeSessionDuration = resumeSessionDuration;
    this.retry = retry;
    this.cleanupStoreOnKeepAlive = cleanupStoreOnKeepAlive;
    this.resumableConnection = resumableDuplexConnection;

    observeDisconnection(initialDuplexConnection);

    S.lazySet(this, Operators.cancelledSubscription());
  }

  void reconnect() {
    if (this.s == Operators.cancelledSubscription()
        && S.compareAndSet(this, Operators.cancelledSubscription(), null)) {
      keepAliveSupport.stop();
      connectionFactory.retryWhen(retry).timeout(resumeSessionDuration).subscribe(this);
    }
  }

  void observeDisconnection(DuplexConnection activeConnection) {
    activeConnection.onClose().subscribe(null, e -> reconnect(), () -> reconnect());
  }

  @Override
  public long impliedPosition() {
    return resumableFramesStore.frameImpliedPosition();
  }

  @Override
  public void onImpliedPosition(long remoteImpliedPos) {
    if (cleanupStoreOnKeepAlive) {
      try {
        resumableFramesStore.releaseFrames(remoteImpliedPos);
      } catch (Throwable e) {
        resumableConnection.sendErrorAndClose(new ConnectionErrorException(e.getMessage(), e));
      }
    }
  }

  @Override
  public void dispose() {
    resumableFramesStore.dispose();
    resumableConnection.dispose();
    resumeToken.release();
  }

  @Override
  public boolean isDisposed() {
    return resumableConnection.isDisposed();
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onNext(Tuple2<ByteBuf, DuplexConnection> tuple2) {
    ByteBuf frame = tuple2.getT1();
    DuplexConnection nextDuplexConnection = tuple2.getT2();

    if (!Operators.terminate(S, this)) {
      logger.debug("Terminating received");
      final ConnectionErrorException connectionErrorException =
          new ConnectionErrorException("resumption_server_pos=[Session Expired]");
      nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      return;
    }

    final FrameType frameType = FrameHeaderCodec.nativeFrameType(frame);
    final int streamId = FrameHeaderCodec.streamId(frame);

    if (streamId != 0) {
      resumableConnection.dispose();
      final ConnectionErrorException connectionErrorException =
          new ConnectionErrorException("Unexpected first frame");
      nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      return;
    }

    if (frameType == FrameType.RESUME_OK) {
      logger.debug("ResumeOK FRAME received");
      long remoteImpliedPos = ResumeOkFrameCodec.lastReceivedClientPos(frame);
      if (resumableFramesStore.framePosition() <= remoteImpliedPos) {
        try {
          resumableFramesStore.releaseFrames(remoteImpliedPos);
        } catch (IllegalStateException e) {
          resumableConnection.dispose();
          final ConnectionErrorException t = new ConnectionErrorException(e.getMessage(), e);
          nextDuplexConnection.sendErrorAndClose(t);
          return;
        }

        if (resumableConnection.connect(nextDuplexConnection)) {
          observeDisconnection(nextDuplexConnection);
          keepAliveSupport.start();
        } else {
          final ConnectionErrorException connectionErrorException =
              new ConnectionErrorException("resumption_server_pos=[Session Expired]");
          nextDuplexConnection.sendErrorAndClose(connectionErrorException);
        }
      } else {
        resumableConnection.dispose();
        final ConnectionErrorException connectionErrorException =
            new ConnectionErrorException("resumption_server_pos=[" + remoteImpliedPos + "]");
        nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      }
    } else if (frameType == FrameType.ERROR) {
      resumableConnection.dispose();
    } else {
      resumableConnection.dispose();
      final ConnectionErrorException connectionErrorException =
          new ConnectionErrorException("Unexpected first frame");
      nextDuplexConnection.sendErrorAndClose(connectionErrorException);
    }
  }

  @Override
  public void onError(Throwable t) {
    if (!Operators.terminate(S, this)) {
      Operators.onErrorDropped(t, currentContext());
    }

    resumableConnection.dispose();
  }

  @Override
  public void onComplete() {}

  public void setKeepAliveSupport(KeepAliveSupport keepAliveSupport) {
    this.keepAliveSupport = keepAliveSupport;
  }

  @Override
  public ByteBuf token() {
    return resumeToken;
  }
}
