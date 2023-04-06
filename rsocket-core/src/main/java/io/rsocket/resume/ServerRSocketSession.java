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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.frame.ResumeFrameCodec;
import io.rsocket.frame.ResumeOkFrameCodec;
import io.rsocket.keepalive.KeepAliveSupport;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

public class ServerRSocketSession
    implements RSocketSession, ResumeStateHolder, CoreSubscriber<Long> {
  private static final Logger logger = LoggerFactory.getLogger(ServerRSocketSession.class);

  final ResumableDuplexConnection resumableConnection;
  final Duration resumeSessionDuration;
  final ResumableFramesStore resumableFramesStore;
  final String session;
  final ByteBufAllocator allocator;
  final boolean cleanupStoreOnKeepAlive;

  /**
   * All incoming connections with the Resume intent are enqueued in this queue. Such an approach
   * ensure that the new connection will affect the resumption state anyhow until the previous
   * (active) connection is finally closed
   */
  final Queue<Runnable> connectionsQueue;

  volatile int wip;
  static final AtomicIntegerFieldUpdater<ServerRSocketSession> WIP =
      AtomicIntegerFieldUpdater.newUpdater(ServerRSocketSession.class, "wip");

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<ServerRSocketSession, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(ServerRSocketSession.class, Subscription.class, "s");

  KeepAliveSupport keepAliveSupport;

  public ServerRSocketSession(
      ByteBuf session,
      ResumableDuplexConnection resumableDuplexConnection,
      DuplexConnection initialDuplexConnection,
      ResumableFramesStore resumableFramesStore,
      Duration resumeSessionDuration,
      boolean cleanupStoreOnKeepAlive) {
    this.session = session.toString(CharsetUtil.UTF_8);
    this.allocator = initialDuplexConnection.alloc();
    this.resumeSessionDuration = resumeSessionDuration;
    this.resumableFramesStore = resumableFramesStore;
    this.cleanupStoreOnKeepAlive = cleanupStoreOnKeepAlive;
    this.resumableConnection = resumableDuplexConnection;
    this.connectionsQueue = Queues.<Runnable>unboundedMultiproducer().get();

    WIP.lazySet(this, 1);

    resumableDuplexConnection.onClose().doFinally(__ -> dispose()).subscribe();
    resumableDuplexConnection.onActiveConnectionClosed().subscribe(__ -> tryTimeoutSession());
  }

  void tryTimeoutSession() {
    keepAliveSupport.stop();

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Side[server]|Session[{}]. Connection is lost. Trying to timeout the active session",
          session);
    }

    Mono.delay(resumeSessionDuration).subscribe(this);

    if (WIP.decrementAndGet(this) == 0) {
      return;
    }

    final Runnable doResumeRunnable = connectionsQueue.poll();
    if (doResumeRunnable != null) {
      doResumeRunnable.run();
    }
  }

  public void resumeWith(ByteBuf resumeFrame, DuplexConnection nextDuplexConnection) {

    if (logger.isDebugEnabled()) {
      logger.debug("Side[server]|Session[{}]. New DuplexConnection received.", session);
    }

    long remotePos = ResumeFrameCodec.firstAvailableClientPos(resumeFrame);
    long remoteImpliedPos = ResumeFrameCodec.lastReceivedServerPos(resumeFrame);

    connectionsQueue.offer(() -> doResume(remotePos, remoteImpliedPos, nextDuplexConnection));

    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    final Runnable doResumeRunnable = connectionsQueue.poll();
    if (doResumeRunnable != null) {
      doResumeRunnable.run();
    }
  }

  void doResume(long remotePos, long remoteImpliedPos, DuplexConnection nextDuplexConnection) {
    if (!tryCancelSessionTimeout()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Side[server]|Session[{}]. Session has already been expired. Terminating received connection",
            session);
      }
      final RejectedResumeException rejectedResumeException =
          new RejectedResumeException("resume_internal_error: Session Expired");
      nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
      nextDuplexConnection.receive().subscribe();
      return;
    }

    long impliedPosition = resumableFramesStore.frameImpliedPosition();
    long position = resumableFramesStore.framePosition();

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Side[server]|Session[{}]. Resume FRAME received. ServerResumeState[impliedPosition[{}], position[{}]]. ClientResumeState[remoteImpliedPosition[{}], remotePosition[{}]]",
          session,
          impliedPosition,
          position,
          remoteImpliedPos,
          remotePos);
    }

    if (remotePos <= impliedPosition && position <= remoteImpliedPos) {
      try {
        if (position != remoteImpliedPos) {
          resumableFramesStore.releaseFrames(remoteImpliedPos);
        }
        nextDuplexConnection.sendFrame(0, ResumeOkFrameCodec.encode(allocator, impliedPosition));
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Side[server]|Session[{}]. ResumeOKFrame[impliedPosition[{}]] has been sent",
              session,
              impliedPosition);
        }
      } catch (Throwable t) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Side[server]|Session[{}]. Exception occurred while releasing frames in the frameStore",
              session,
              t);
        }

        dispose();

        final RejectedResumeException rejectedResumeException =
            new RejectedResumeException(t.getMessage(), t);
        nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
        nextDuplexConnection.receive().subscribe();

        return;
      }

      keepAliveSupport.start();

      if (logger.isDebugEnabled()) {
        logger.debug("Side[server]|Session[{}]. Session has been resumed successfully", session);
      }

      if (!resumableConnection.connect(nextDuplexConnection)) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Side[server]|Session[{}]. Session has already been expired. Terminating received connection",
              session);
        }
        final RejectedResumeException rejectedResumeException =
            new RejectedResumeException("resume_internal_error: Session Expired");
        nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
        nextDuplexConnection.receive().subscribe();

        // resumableConnection is likely to be disposed at this stage. Thus we have
        // nothing to do
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Side[server]|Session[{}]. Mismatching remote and local state. Expected RemoteImpliedPosition[{}] to be greater or equal to the LocalPosition[{}] and RemotePosition[{}] to be less or equal to LocalImpliedPosition[{}]. Terminating received connection",
            session,
            remoteImpliedPos,
            position,
            remotePos,
            impliedPosition);
      }

      dispose();

      final RejectedResumeException rejectedResumeException =
          new RejectedResumeException(
              String.format(
                  "resumption_pos=[ remote: { pos: %d, impliedPos: %d }, local: { pos: %d, impliedPos: %d }]",
                  remotePos, remoteImpliedPos, position, impliedPosition));
      nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
      nextDuplexConnection.receive().subscribe();
    }
  }

  boolean tryCancelSessionTimeout() {
    for (; ; ) {
      final Subscription subscription = this.s;

      if (subscription == Operators.cancelledSubscription()) {
        return false;
      }

      if (S.compareAndSet(this, subscription, null)) {
        subscription.cancel();
        return true;
      }
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
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onNext(Long aLong) {
    if (!Operators.terminate(S, this)) {
      return;
    }

    resumableConnection.dispose();
  }

  @Override
  public void onComplete() {}

  @Override
  public void onError(Throwable t) {}

  public void setKeepAliveSupport(KeepAliveSupport keepAliveSupport) {
    this.keepAliveSupport = keepAliveSupport;
  }

  @Override
  public void dispose() {
    Operators.terminate(S, this);
    resumableConnection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return resumableConnection.isDisposed();
  }
}
