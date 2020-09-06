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
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.frame.ResumeFrameCodec;
import io.rsocket.frame.ResumeOkFrameCodec;
import io.rsocket.keepalive.KeepAliveSupport;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

public class ServerRSocketSession
    implements RSocketSession, ResumeStateHolder, CoreSubscriber<Long> {
  private static final Logger logger = LoggerFactory.getLogger(ServerRSocketSession.class);

  final ResumableDuplexConnection resumableConnection;
  final Duration resumeSessionDuration;
  final ResumableFramesStore resumableFramesStore;
  final ByteBufAllocator allocator;
  final ByteBuf resumeToken;
  final boolean cleanupStoreOnKeepAlive;

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<ServerRSocketSession, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(ServerRSocketSession.class, Subscription.class, "s");

  KeepAliveSupport keepAliveSupport;

  public ServerRSocketSession(
      DuplexConnection initialDuplexConnection,
      ResumableDuplexConnection resumableDuplexConnection,
      Duration resumeSessionDuration,
      ResumableFramesStore resumableFramesStore,
      ByteBuf resumeToken,
      boolean cleanupStoreOnKeepAlive) {
    this.allocator = initialDuplexConnection.alloc();
    this.resumeSessionDuration = resumeSessionDuration;
    this.resumableFramesStore = resumableFramesStore;
    this.resumableConnection = resumableDuplexConnection;
    this.resumeToken = resumeToken;
    this.cleanupStoreOnKeepAlive = cleanupStoreOnKeepAlive;

    observeDisconnection(initialDuplexConnection);
  }

  void observeDisconnection(DuplexConnection activeConnection) {
    activeConnection.onClose().subscribe(null, e -> tryTimeoutSession(), () -> tryTimeoutSession());
  }

  void tryTimeoutSession() {
    keepAliveSupport.stop();
    Mono.delay(resumeSessionDuration).subscribe(this);
  }

  public Mono<Void> resumeWith(ByteBuf resumeFrame, DuplexConnection nextDuplexConnection) {
    logger.debug("Resume FRAME received");

    for (; ; ) {
      final Subscription subscription = this.s;

      if (subscription == Operators.cancelledSubscription()) {
        final RejectedResumeException rejectedResumeException =
            new RejectedResumeException("resume_internal_error: Session Expired");
        nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
        return nextDuplexConnection.onClose();
      }

      if (S.compareAndSet(this, subscription, null)) {
        subscription.cancel();
        break;
      }
    }

    long remotePos = ResumeFrameCodec.firstAvailableClientPos(resumeFrame);
    long remoteImpliedPos = ResumeFrameCodec.lastReceivedServerPos(resumeFrame);
    long impliedPosition = resumableFramesStore.frameImpliedPosition();
    long position = resumableFramesStore.framePosition();

    if (remotePos <= impliedPosition && position <= remoteImpliedPos) {
      try {
        resumableFramesStore.releaseFrames(remoteImpliedPos);
        nextDuplexConnection.sendFrame(0, ResumeOkFrameCodec.encode(allocator, impliedPosition));
        logger.debug("ResumeOk has been sent");
      } catch (Throwable t) {
        resumableConnection.dispose();
        nextDuplexConnection.sendErrorAndClose(new RejectedResumeException(t.getMessage(), t));
        return nextDuplexConnection.onClose();
      }
      if (resumableConnection.connect(nextDuplexConnection)) {
        observeDisconnection(nextDuplexConnection);
        keepAliveSupport.start();
      } else {
        final RejectedResumeException rejectedResumeException =
            new RejectedResumeException("resume_internal_error: Session Expired");
        nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
      }
    } else {
      resumableConnection.dispose();
      final RejectedResumeException rejectedResumeException =
          new RejectedResumeException(
              String.format(
                  "resumption_pos=[ remote: { pos: %d, impliedPos: %d }, local: { pos: %d, impliedPos: %d }]",
                  remotePos, remoteImpliedPos, position, impliedPosition));
      nextDuplexConnection.sendErrorAndClose(rejectedResumeException);
    }

    return nextDuplexConnection.onClose();
  }

  @Override
  public ByteBuf token() {
    return resumeToken;
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
    resumableFramesStore.dispose();
    resumableConnection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return resumableConnection.isDisposed();
  }

  private static RejectedResumeException errorFrameThrowable(Throwable err) {
    String msg;
    if (err instanceof ResumeStateException) {
      ResumeStateException resumeException = ((ResumeStateException) err);
      msg =
          String.format(
              "resumption_pos=[ remote: { pos: %d, impliedPos: %d }, local: { pos: %d, impliedPos: %d }]",
              resumeException.getRemotePos(),
              resumeException.getRemoteImpliedPos(),
              resumeException.getLocalPos(),
              resumeException.getLocalImpliedPos());
    } else {
      msg = String.format("resume_internal_error: %s", err.getMessage());
    }
    return new RejectedResumeException(msg);
  }
}
