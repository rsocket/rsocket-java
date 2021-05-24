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
import io.netty.util.CharsetUtil;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.Exceptions;
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
  final ByteBuf resumeToken;
  final String session;

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<ClientRSocketSession, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(ClientRSocketSession.class, Subscription.class, "s");

  KeepAliveSupport keepAliveSupport;

  public ClientRSocketSession(
      ByteBuf resumeToken,
      ResumableDuplexConnection resumableDuplexConnection,
      Mono<DuplexConnection> connectionFactory,
      Function<DuplexConnection, Mono<Tuple2<ByteBuf, DuplexConnection>>> connectionTransformer,
      ResumableFramesStore resumableFramesStore,
      Duration resumeSessionDuration,
      Retry retry,
      boolean cleanupStoreOnKeepAlive) {
    this.resumeToken = resumeToken;
    this.session = resumeToken.toString(CharsetUtil.UTF_8);
    this.connectionFactory =
        connectionFactory.flatMap(
            dc -> {
              final long impliedPosition = resumableFramesStore.frameImpliedPosition();
              final long position = resumableFramesStore.framePosition();
              dc.sendFrame(
                  0,
                  ResumeFrameCodec.encode(
                      dc.alloc(),
                      resumeToken.retain(),
                      // server uses this to release its cache
                      impliedPosition, //  observed on the client side
                      // server uses this to check whether there is no mismatch
                      position //  sent from the client sent
                      ));

              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Side[client]|Session[{}]. ResumeFrame[impliedPosition[{}], position[{}]] has been sent.",
                    session,
                    impliedPosition,
                    position);
              }

              return connectionTransformer.apply(dc);
            });
    this.resumableFramesStore = resumableFramesStore;
    this.allocator = resumableDuplexConnection.alloc();
    this.resumeSessionDuration = resumeSessionDuration;
    this.retry = retry;
    this.cleanupStoreOnKeepAlive = cleanupStoreOnKeepAlive;
    this.resumableConnection = resumableDuplexConnection;

    resumableDuplexConnection.onClose().doFinally(__ -> dispose()).subscribe();
    resumableDuplexConnection.onActiveConnectionClosed().subscribe(this::reconnect);

    S.lazySet(this, Operators.cancelledSubscription());
  }

  void reconnect(int index) {
    if (this.s == Operators.cancelledSubscription()
        && S.compareAndSet(this, Operators.cancelledSubscription(), null)) {
      keepAliveSupport.stop();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Side[client]|Session[{}]. Connection[{}] is lost. Reconnecting to resume...",
            session,
            index);
      }
      connectionFactory.retryWhen(retry).timeout(resumeSessionDuration).subscribe(this);
    }
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
    Operators.terminate(S, this);
    resumableConnection.dispose();
    resumableFramesStore.dispose();

    if (resumeToken.refCnt() > 0) {
      resumeToken.release();
    }
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
    ByteBuf shouldBeResumeOKFrame = tuple2.getT1();
    DuplexConnection nextDuplexConnection = tuple2.getT2();

    if (!Operators.terminate(S, this)) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Side[client]|Session[{}]. Session has already been expired. Terminating received connection",
            session);
      }
      final ConnectionErrorException connectionErrorException =
          new ConnectionErrorException("resumption_server=[Session Expired]");
      nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      nextDuplexConnection.receive().subscribe().dispose();
      return;
    }

    final int streamId = FrameHeaderCodec.streamId(shouldBeResumeOKFrame);
    if (streamId != 0) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Side[client]|Session[{}]. Illegal first frame received. RESUME_OK frame must be received before any others. Terminating received connection",
            session);
      }
      final ConnectionErrorException connectionErrorException =
          new ConnectionErrorException("RESUME_OK frame must be received before any others");
      resumableConnection.dispose(connectionErrorException);
      nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      nextDuplexConnection.receive().subscribe().dispose();
      return;
    }

    final FrameType frameType = FrameHeaderCodec.nativeFrameType(shouldBeResumeOKFrame);
    if (frameType == FrameType.RESUME_OK) {
      // how  many frames the server has received from the client
      // so the client can release cached frames by this point
      long remoteImpliedPos = ResumeOkFrameCodec.lastReceivedClientPos(shouldBeResumeOKFrame);
      // what was the last notification from the server about number of frames being
      // observed
      final long position = resumableFramesStore.framePosition();
      final long impliedPosition = resumableFramesStore.frameImpliedPosition();
      logger.debug(
          "Side[client]|Session[{}]. ResumeOK FRAME received. ServerResumeState[remoteImpliedPosition[{}]]. ClientResumeState[impliedPosition[{}], position[{}]]",
          session,
          remoteImpliedPos,
          impliedPosition,
          position);
      if (position <= remoteImpliedPos) {
        try {
          if (position != remoteImpliedPos) {
            resumableFramesStore.releaseFrames(remoteImpliedPos);
          }
        } catch (IllegalStateException e) {
          logger.debug("Exception occurred while releasing frames in the frameStore", e);
          resumableConnection.dispose(e);
          final ConnectionErrorException t = new ConnectionErrorException(e.getMessage(), e);
          nextDuplexConnection.sendErrorAndClose(t);
          nextDuplexConnection.receive().subscribe().dispose();
          return;
        }

        if (resumableConnection.connect(nextDuplexConnection)) {
          keepAliveSupport.start();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Side[client]|Session[{}]. Session has been resumed successfully", session);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Side[client]|Session[{}]. Session has already been expired. Terminating received connection",
                session);
          }
          final ConnectionErrorException connectionErrorException =
              new ConnectionErrorException("resumption_server_pos=[Session Expired]");
          nextDuplexConnection.sendErrorAndClose(connectionErrorException);
          nextDuplexConnection.receive().subscribe().dispose();
        }
      } else {
        logger.debug(
            "Side[client]|Session[{}]. Mismatching remote and local state. Expected RemoteImpliedPosition[{}] to be greater or equal to the LocalPosition[{}]. Terminating received connection",
            session,
            remoteImpliedPos,
            position);
        final ConnectionErrorException connectionErrorException =
            new ConnectionErrorException("resumption_server_pos=[" + remoteImpliedPos + "]");
        resumableConnection.dispose(connectionErrorException);
        nextDuplexConnection.sendErrorAndClose(connectionErrorException);
        nextDuplexConnection.receive().subscribe().dispose();
      }
    } else if (frameType == FrameType.ERROR) {
      final RuntimeException exception = Exceptions.from(0, shouldBeResumeOKFrame);
      logger.debug("Received error frame. Terminating received connection", exception);
      resumableConnection.dispose(exception);
    } else {
      logger.debug(
          "Illegal first frame received. RESUME_OK frame must be received before any others. Terminating received connection");
      final ConnectionErrorException connectionErrorException =
          new ConnectionErrorException("RESUME_OK frame must be received before any others");
      resumableConnection.dispose(connectionErrorException);
      nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      nextDuplexConnection.receive().subscribe().dispose();
    }
  }

  @Override
  public void onError(Throwable t) {
    if (!Operators.terminate(S, this)) {
      Operators.onErrorDropped(t, currentContext());
    }

    resumableConnection.dispose(t);
  }

  @Override
  public void onComplete() {}

  public void setKeepAliveSupport(KeepAliveSupport keepAliveSupport) {
    this.keepAliveSupport = keepAliveSupport;
  }
}
