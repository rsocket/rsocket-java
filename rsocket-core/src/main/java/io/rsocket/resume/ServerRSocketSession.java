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
  final String resumeToken;
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
      ByteBuf resumeToken,
      DuplexConnection initialDuplexConnection,
      ResumableDuplexConnection resumableDuplexConnection,
      Duration resumeSessionDuration,
      ResumableFramesStore resumableFramesStore,
      boolean cleanupStoreOnKeepAlive) {
    this.resumeToken = resumeToken.toString(CharsetUtil.UTF_8);
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
    Mono.delay(resumeSessionDuration).subscribe(this);
    logger.debug("Connection is lost. Trying to timeout the active session[{}]", resumeToken);

    if (WIP.decrementAndGet(this) == 0) {
      return;
    }

    final Runnable doResumeRunnable = connectionsQueue.poll();
    if (doResumeRunnable != null) {
      doResumeRunnable.run();
    }
  }

  public void resumeWith(ByteBuf resumeFrame, DuplexConnection nextDuplexConnection) {

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

    long impliedPosition = resumableFramesStore.frameImpliedPosition();
    long position = resumableFramesStore.framePosition();

    logger.debug(
        "Resume FRAME received. ClientResumeState{observedFramesPosition[{}], sentFramesPosition[{}]}, ServerResumeState{observedFramesPosition[{}], sentFramesPosition[{}]}",
        remoteImpliedPos,
        remotePos,
        impliedPosition,
        position);

    for (; ; ) {
      final Subscription subscription = this.s;

      if (subscription == Operators.cancelledSubscription()) {
        logger.debug("Session has already been expired. Terminating received connection");
        final RejectedResumeException rejectedResumeException =
            new RejectedResumeException("resume_internal_error: Session Expired");
        nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
        return;
      }

      if (S.compareAndSet(this, subscription, null)) {
        subscription.cancel();
        break;
      }
    }

    if (remotePos <= impliedPosition && position <= remoteImpliedPos) {
      try {
        if (position != remoteImpliedPos) {
          resumableFramesStore.releaseFrames(remoteImpliedPos);
        }
        nextDuplexConnection.sendFrame(
            0, ResumeOkFrameCodec.encode(allocator, resumableFramesStore.frameImpliedPosition()));
        logger.debug("ResumeOK Frame has been sent");
      } catch (Throwable t) {
        logger.debug("Exception occurred while releasing frames in the frameStore", t);
        tryTimeoutSession();
        nextDuplexConnection.sendErrorAndClose(new RejectedResumeException(t.getMessage(), t));
        return;
      }
      if (resumableConnection.connect(nextDuplexConnection)) {
        keepAliveSupport.start();
        logger.debug("Session[{}] has been resumed successfully", resumeToken);
      } else {
        logger.debug("Session has already been expired. Terminating received connection");
        final ConnectionErrorException connectionErrorException =
            new ConnectionErrorException("resume_internal_error: Session Expired");
        nextDuplexConnection.sendErrorAndClose(connectionErrorException);
      }
    } else {
      logger.debug(
          "Mismatching remote and local state. Expected RemoteImpliedPosition[{}] to be greater or equal to the LocalPosition[{}] and RemotePosition[{}] to be less or equal to LocalImpliedPosition[{}]. Terminating received connection",
          remoteImpliedPos,
          position,
          remotePos,
          impliedPosition);
      tryTimeoutSession();
      final RejectedResumeException rejectedResumeException =
          new RejectedResumeException(
              String.format(
                  "resumption_pos=[ remote: { pos: %d, impliedPos: %d }, local: { pos: %d, impliedPos: %d }]",
                  remotePos, remoteImpliedPos, position, impliedPosition));
      nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
    }
  }

  @Override
  public long impliedPosition() {
    return resumableFramesStore.frameImpliedPosition();
  }

  @Override
  public void onImpliedPosition(long remoteImpliedPos) {
    if (cleanupStoreOnKeepAlive) {
      resumableFramesStore.releaseFrames(remoteImpliedPos);
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
    resumableFramesStore.dispose();
  }

  @Override
  public boolean isDisposed() {
    return resumableConnection.isDisposed();
  }
}
