/*
 * Copyright 2015-2021 the original author or authors.
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

import static io.rsocket.resume.ResumableDuplexConnection.isResumableFrame;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

/**
 * writes - n (where n is frequent, primary operation) reads - m (where m == KeepAliveFrequency)
 * skip - k -> 0 (where k is the rare operation which happens after disconnection
 */
public class InMemoryResumableFramesStore extends Flux<ByteBuf>
    implements ResumableFramesStore, Subscription {

  private FramesSubscriber framesSubscriber;
  private static final Logger logger = LoggerFactory.getLogger(InMemoryResumableFramesStore.class);

  final Sinks.Empty<Void> disposed = Sinks.empty();
  final Queue<ByteBuf> cachedFrames;
  final String side;
  final String session;
  final int cacheLimit;

  volatile long impliedPosition;
  static final AtomicLongFieldUpdater<InMemoryResumableFramesStore> IMPLIED_POSITION =
      AtomicLongFieldUpdater.newUpdater(InMemoryResumableFramesStore.class, "impliedPosition");

  volatile long firstAvailableFramePosition;
  static final AtomicLongFieldUpdater<InMemoryResumableFramesStore> FIRST_AVAILABLE_FRAME_POSITION =
      AtomicLongFieldUpdater.newUpdater(
          InMemoryResumableFramesStore.class, "firstAvailableFramePosition");

  long remoteImpliedPosition;

  int cacheSize;

  Throwable terminal;

  CoreSubscriber<? super ByteBuf> actual;
  CoreSubscriber<? super ByteBuf> pendingActual;

  volatile long state;
  static final AtomicLongFieldUpdater<InMemoryResumableFramesStore> STATE =
      AtomicLongFieldUpdater.newUpdater(InMemoryResumableFramesStore.class, "state");

  /**
   * Flag which indicates that {@link InMemoryResumableFramesStore} is finalized and all related
   * stores are cleaned
   */
  static final long FINALIZED_FLAG =
      0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /**
   * Flag which indicates that {@link InMemoryResumableFramesStore} is terminated via the {@link
   * InMemoryResumableFramesStore#dispose()} method
   */
  static final long DISPOSED_FLAG =
      0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /**
   * Flag which indicates that {@link InMemoryResumableFramesStore} is terminated via the {@link
   * FramesSubscriber#onComplete()} or {@link FramesSubscriber#onError(Throwable)} ()} methods
   */
  static final long TERMINATED_FLAG =
      0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /** Flag which indicates that {@link InMemoryResumableFramesStore} has active frames consumer */
  static final long CONNECTED_FLAG =
      0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /**
   * Flag which indicates that {@link InMemoryResumableFramesStore} has no active frames consumer
   * but there is a one pending
   */
  static final long PENDING_CONNECTION_FLAG =
      0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /**
   * Flag which indicates that there are some received implied position changes from the remote
   * party
   */
  static final long REMOTE_IMPLIED_POSITION_CHANGED_FLAG =
      0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /**
   * Flag which indicates that there are some frames stored in the {@link
   * io.rsocket.internal.UnboundedProcessor} which has to be cached and sent to the remote party
   */
  static final long HAS_FRAME_FLAG =
      0b0000_0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  /**
   * Flag which indicates that {@link InMemoryResumableFramesStore#drain(long)} has an actor which
   * is currently progressing on the work. This flag should work as a guard to enter|exist into|from
   * the {@link InMemoryResumableFramesStore#drain(long)} method.
   */
  static final long MAX_WORK_IN_PROGRESS =
      0b0000_0000_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111L;

  public InMemoryResumableFramesStore(String side, ByteBuf session, int cacheSizeBytes) {
    this.side = side;
    this.session = session.toString(CharsetUtil.UTF_8);
    this.cacheLimit = cacheSizeBytes;
    this.cachedFrames = new ArrayDeque<>();
  }

  public Mono<Void> saveFrames(Flux<ByteBuf> frames) {
    return frames
        .transform(
            Operators.<ByteBuf, Void>lift(
                (__, actual) -> this.framesSubscriber = new FramesSubscriber(actual, this)))
        .then();
  }

  @Override
  public void releaseFrames(long remoteImpliedPos) {
    long lastReceivedRemoteImpliedPosition = this.remoteImpliedPosition;
    if (lastReceivedRemoteImpliedPosition > remoteImpliedPos) {
      throw new IllegalStateException(
          "Given Remote Implied Position is behind the last received Remote Implied Position");
    }

    this.remoteImpliedPosition = remoteImpliedPos;

    final long previousState = markRemoteImpliedPositionChanged(this);
    if (isFinalized(previousState) || isWorkInProgress(previousState)) {
      return;
    }

    drain((previousState + 1) | REMOTE_IMPLIED_POSITION_CHANGED_FLAG);
  }

  void drain(long expectedState) {
    final Fuseable.QueueSubscription<ByteBuf> qs = this.framesSubscriber.qs;
    final Queue<ByteBuf> cachedFrames = this.cachedFrames;

    for (; ; ) {
      if (hasRemoteImpliedPositionChanged(expectedState)) {
        expectedState = handlePendingRemoteImpliedPositionChanges(expectedState, cachedFrames);
      }

      if (hasPendingConnection(expectedState)) {
        expectedState = handlePendingConnection(expectedState, cachedFrames);
      }

      if (isConnected(expectedState)) {
        if (isTerminated(expectedState)) {
          handleTerminated(qs, this.terminal);
        } else if (isDisposed()) {
          handleDisposed();
        } else if (hasFrames(expectedState)) {
          handlePendingFrames(qs);
        }
      }

      if (isDisposed(expectedState) || isTerminated(expectedState)) {
        clearAndFinalize(this);
        return;
      }

      expectedState = markWorkDone(this, expectedState);
      if (isFinalized(expectedState)) {
        return;
      }

      if (!isWorkInProgress(expectedState)) {
        return;
      }
    }
  }

  long handlePendingRemoteImpliedPositionChanges(long expectedState, Queue<ByteBuf> cachedFrames) {
    final long remoteImpliedPosition = this.remoteImpliedPosition;
    final long firstAvailableFramePosition = this.firstAvailableFramePosition;
    final long toDropFromCache = Math.max(0, remoteImpliedPosition - firstAvailableFramePosition);

    if (toDropFromCache > 0) {
      final int droppedFromCache = dropFramesFromCache(toDropFromCache, cachedFrames);

      if (toDropFromCache > droppedFromCache) {
        this.terminal =
            new IllegalStateException(
                String.format(
                    "Local and remote state disagreement: "
                        + "need to remove additional %d bytes, but cache is empty",
                    toDropFromCache));
        expectedState = markTerminated(this) | TERMINATED_FLAG;
      }

      if (toDropFromCache < droppedFromCache) {
        this.terminal =
            new IllegalStateException(
                "Local and remote state disagreement: local and remote frame sizes are not equal");
        expectedState = markTerminated(this) | TERMINATED_FLAG;
      }

      FIRST_AVAILABLE_FRAME_POSITION.lazySet(this, firstAvailableFramePosition + droppedFromCache);
      if (this.cacheLimit != Integer.MAX_VALUE) {
        this.cacheSize -= droppedFromCache;

        if (logger.isDebugEnabled()) {
          logger.debug(
              "Side[{}]|Session[{}]. Removed frames from cache to position[{}]. CacheSize[{}]",
              this.side,
              this.session,
              this.remoteImpliedPosition,
              this.cacheSize);
        }
      }
    }

    return expectedState;
  }

  void handlePendingFrames(Fuseable.QueueSubscription<ByteBuf> qs) {
    for (; ; ) {
      final ByteBuf frame = qs.poll();
      final boolean empty = frame == null;

      if (empty) {
        break;
      }

      handleFrame(frame);

      if (!isConnected(this.state)) {
        break;
      }
    }
  }

  long handlePendingConnection(long expectedState, Queue<ByteBuf> cachedFrames) {
    CoreSubscriber<? super ByteBuf> lastActual = null;
    for (; ; ) {
      final CoreSubscriber<? super ByteBuf> nextActual = this.pendingActual;

      if (nextActual != lastActual) {
        for (final ByteBuf frame : cachedFrames) {
          nextActual.onNext(frame.retainedSlice());
        }
      }

      expectedState = markConnected(this, expectedState);
      if (isConnected(expectedState)) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Side[{}]|Session[{}]. Connected at Position[{}] and ImpliedPosition[{}]",
              side,
              session,
              firstAvailableFramePosition,
              impliedPosition);
        }

        this.actual = nextActual;
        break;
      }

      if (!hasPendingConnection(expectedState)) {
        break;
      }

      lastActual = nextActual;
    }
    return expectedState;
  }

  static int dropFramesFromCache(long toRemoveBytes, Queue<ByteBuf> cache) {
    int removedBytes = 0;
    while (toRemoveBytes > removedBytes && cache.size() > 0) {
      final ByteBuf cachedFrame = cache.poll();
      final int frameSize = cachedFrame.readableBytes();

      cachedFrame.release();

      removedBytes += frameSize;
    }

    return removedBytes;
  }

  @Override
  public Flux<ByteBuf> resumeStream() {
    return this;
  }

  @Override
  public long framePosition() {
    return this.firstAvailableFramePosition;
  }

  @Override
  public long frameImpliedPosition() {
    return this.impliedPosition & Long.MAX_VALUE;
  }

  @Override
  public boolean resumableFrameReceived(ByteBuf frame) {
    final int frameSize = frame.readableBytes();
    for (; ; ) {
      final long impliedPosition = this.impliedPosition;

      if (impliedPosition < 0) {
        return false;
      }

      if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, impliedPosition + frameSize)) {
        return true;
      }
    }
  }

  void pauseImplied() {
    for (; ; ) {
      final long impliedPosition = this.impliedPosition;

      if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, impliedPosition | Long.MIN_VALUE)) {
        logger.debug(
            "Side[{}]|Session[{}]. Paused at position[{}]", side, session, impliedPosition);
        return;
      }
    }
  }

  void resumeImplied() {
    for (; ; ) {
      final long impliedPosition = this.impliedPosition;

      final long restoredImpliedPosition = impliedPosition & Long.MAX_VALUE;
      if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, restoredImpliedPosition)) {
        logger.debug(
            "Side[{}]|Session[{}]. Resumed at position[{}]",
            side,
            session,
            restoredImpliedPosition);
        return;
      }
    }
  }

  @Override
  public Mono<Void> onClose() {
    return disposed.asMono();
  }

  @Override
  public void dispose() {
    final long previousState = markDisposed(this);
    if (isFinalized(previousState)
        || isDisposed(previousState)
        || isWorkInProgress(previousState)) {
      return;
    }

    drain((previousState + 1) | DISPOSED_FLAG);
  }

  void clearCache() {
    final Queue<ByteBuf> frames = this.cachedFrames;
    this.cacheSize = 0;

    ByteBuf frame;
    while ((frame = frames.poll()) != null) {
      frame.release();
    }
  }

  @Override
  public boolean isDisposed() {
    return isDisposed(this.state);
  }

  void handleFrame(ByteBuf frame) {
    final boolean isResumable = isResumableFrame(frame);
    if (isResumable) {
      handleResumableFrame(frame);
      return;
    }

    handleConnectionFrame(frame);
  }

  void handleTerminated(Fuseable.QueueSubscription<ByteBuf> qs, @Nullable Throwable t) {
    for (; ; ) {
      final ByteBuf frame = qs.poll();
      final boolean empty = frame == null;

      if (empty) {
        break;
      }

      handleFrame(frame);
    }
    if (t != null) {
      this.actual.onError(t);
    } else {
      this.actual.onComplete();
    }
  }

  void handleDisposed() {
    this.actual.onError(new CancellationException("Disposed"));
  }

  void handleConnectionFrame(ByteBuf frame) {
    this.actual.onNext(frame);
  }

  void handleResumableFrame(ByteBuf frame) {
    final Queue<ByteBuf> frames = this.cachedFrames;
    final int incomingFrameSize = frame.readableBytes();
    final int cacheLimit = this.cacheLimit;

    final boolean canBeStore;
    int cacheSize = this.cacheSize;
    if (cacheLimit != Integer.MAX_VALUE) {
      final long availableSize = cacheLimit - cacheSize;

      if (availableSize < incomingFrameSize) {
        final long firstAvailableFramePosition = this.firstAvailableFramePosition;
        final long toRemoveBytes = incomingFrameSize - availableSize;
        final int removedBytes = dropFramesFromCache(toRemoveBytes, frames);

        cacheSize = cacheSize - removedBytes;
        canBeStore = removedBytes >= toRemoveBytes;

        if (canBeStore) {
          FIRST_AVAILABLE_FRAME_POSITION.lazySet(this, firstAvailableFramePosition + removedBytes);
        } else {
          this.cacheSize = cacheSize;
          FIRST_AVAILABLE_FRAME_POSITION.lazySet(
              this, firstAvailableFramePosition + removedBytes + incomingFrameSize);
        }
      } else {
        canBeStore = true;
      }
    } else {
      canBeStore = true;
    }

    if (canBeStore) {
      frames.offer(frame);

      if (cacheLimit != Integer.MAX_VALUE) {
        this.cacheSize = cacheSize + incomingFrameSize;
      }
    }

    this.actual.onNext(canBeStore ? frame.retainedSlice() : frame);
  }

  @Override
  public void request(long n) {}

  @Override
  public void cancel() {
    pauseImplied();
    markDisconnected(this);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Side[{}]|Session[{}]. Disconnected at Position[{}] and ImpliedPosition[{}]",
          side,
          session,
          firstAvailableFramePosition,
          frameImpliedPosition());
    }
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    resumeImplied();
    actual.onSubscribe(this);
    this.pendingActual = actual;

    final long previousState = markPendingConnection(this);
    if (isDisposed(previousState)) {
      actual.onError(new CancellationException("Disposed"));
      return;
    }

    if (isTerminated(previousState)) {
      actual.onError(new CancellationException("Disposed"));
      return;
    }

    if (isWorkInProgress(previousState)) {
      return;
    }

    drain((previousState + 1) | PENDING_CONNECTION_FLAG);
  }

  static class FramesSubscriber
      implements CoreSubscriber<ByteBuf>, Fuseable.QueueSubscription<Void> {

    final CoreSubscriber<? super Void> actual;
    final InMemoryResumableFramesStore parent;

    Fuseable.QueueSubscription<ByteBuf> qs;

    boolean done;

    FramesSubscriber(CoreSubscriber<? super Void> actual, InMemoryResumableFramesStore parent) {
      this.actual = actual;
      this.parent = parent;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.qs, s)) {
        final Fuseable.QueueSubscription<ByteBuf> qs = (Fuseable.QueueSubscription<ByteBuf>) s;
        this.qs = qs;

        final int m = qs.requestFusion(Fuseable.ANY);

        if (m != Fuseable.ASYNC) {
          s.cancel();
          this.actual.onSubscribe(this);
          this.actual.onError(new IllegalStateException("Source has to be ASYNC fuseable"));
          return;
        }

        this.actual.onSubscribe(this);
      }
    }

    @Override
    public void onNext(ByteBuf byteBuf) {
      final InMemoryResumableFramesStore parent = this.parent;
      long previousState = InMemoryResumableFramesStore.markFrameAdded(parent);

      if (isFinalized(previousState)) {
        this.qs.clear();
        return;
      }

      if (isWorkInProgress(previousState)) {
        return;
      }

      if (isConnected(previousState) || hasPendingConnection(previousState)) {
        parent.drain((previousState + 1) | HAS_FRAME_FLAG);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      final InMemoryResumableFramesStore parent = this.parent;

      parent.terminal = t;
      this.done = true;

      final long previousState = InMemoryResumableFramesStore.markTerminated(parent);
      if (isFinalized(previousState)) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      if (isWorkInProgress(previousState)) {
        return;
      }

      parent.drain((previousState + 1) | TERMINATED_FLAG);
    }

    @Override
    public void onComplete() {
      if (this.done) {
        return;
      }

      final InMemoryResumableFramesStore parent = this.parent;

      this.done = true;

      final long previousState = InMemoryResumableFramesStore.markTerminated(parent);
      if (isFinalized(previousState)) {
        return;
      }

      if (isWorkInProgress(previousState)) {
        return;
      }

      parent.drain((previousState + 1) | TERMINATED_FLAG);
    }

    @Override
    public void cancel() {
      if (this.done) {
        return;
      }

      this.done = true;

      final long previousState = InMemoryResumableFramesStore.markTerminated(parent);
      if (isFinalized(previousState)) {
        return;
      }

      if (isWorkInProgress(previousState)) {
        return;
      }

      parent.drain(previousState | TERMINATED_FLAG);
    }

    @Override
    public void request(long n) {}

    @Override
    public int requestFusion(int requestedMode) {
      return Fuseable.NONE;
    }

    @Override
    public Void poll() {
      return null;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public void clear() {}
  }

  static long markFrameAdded(InMemoryResumableFramesStore store) {
    for (; ; ) {
      final long state = store.state;

      if (isFinalized(state)) {
        return state;
      }

      long nextState = state;
      if (isConnected(state) || hasPendingConnection(state) || isWorkInProgress(state)) {
        nextState =
            (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? nextState : nextState + 1;
      }

      if (STATE.compareAndSet(store, state, nextState | HAS_FRAME_FLAG)) {
        return state;
      }
    }
  }

  static long markPendingConnection(InMemoryResumableFramesStore store) {
    for (; ; ) {
      final long state = store.state;

      if (isFinalized(state) || isDisposed(state) || isTerminated(state)) {
        return state;
      }

      if (isConnected(state)) {
        return state;
      }

      final long nextState =
          (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : state + 1;
      if (STATE.compareAndSet(store, state, nextState | PENDING_CONNECTION_FLAG)) {
        return state;
      }
    }
  }

  static long markRemoteImpliedPositionChanged(InMemoryResumableFramesStore store) {
    for (; ; ) {
      final long state = store.state;

      if (isFinalized(state)) {
        return state;
      }

      final long nextState =
          (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : (state + 1);
      if (STATE.compareAndSet(store, state, nextState | REMOTE_IMPLIED_POSITION_CHANGED_FLAG)) {
        return state;
      }
    }
  }

  static long markDisconnected(InMemoryResumableFramesStore store) {
    for (; ; ) {
      final long state = store.state;

      if (isFinalized(state)) {
        return state;
      }

      if (STATE.compareAndSet(store, state, state & ~CONNECTED_FLAG & ~PENDING_CONNECTION_FLAG)) {
        return state;
      }
    }
  }

  static long markWorkDone(InMemoryResumableFramesStore store, long expectedState) {
    for (; ; ) {
      final long state = store.state;

      if (expectedState != state) {
        return state;
      }

      if (isFinalized(state)) {
        return state;
      }

      final long nextState = state & ~MAX_WORK_IN_PROGRESS & ~REMOTE_IMPLIED_POSITION_CHANGED_FLAG;
      if (STATE.compareAndSet(store, state, nextState)) {
        return nextState;
      }
    }
  }

  static long markConnected(InMemoryResumableFramesStore store, long expectedState) {
    for (; ; ) {
      final long state = store.state;

      if (state != expectedState) {
        return state;
      }

      if (isFinalized(state)) {
        return state;
      }

      final long nextState = state ^ PENDING_CONNECTION_FLAG | CONNECTED_FLAG;
      if (STATE.compareAndSet(store, state, nextState)) {
        return nextState;
      }
    }
  }

  static long markTerminated(InMemoryResumableFramesStore store) {
    for (; ; ) {
      final long state = store.state;

      if (isFinalized(state)) {
        return state;
      }

      final long nextState =
          (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : (state + 1);
      if (STATE.compareAndSet(store, state, nextState | TERMINATED_FLAG)) {
        return state;
      }
    }
  }

  static long markDisposed(InMemoryResumableFramesStore store) {
    for (; ; ) {
      final long state = store.state;

      if (isFinalized(state)) {
        return state;
      }

      final long nextState =
          (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : (state + 1);
      if (STATE.compareAndSet(store, state, nextState | DISPOSED_FLAG)) {
        return state;
      }
    }
  }

  static void clearAndFinalize(InMemoryResumableFramesStore store) {
    final Fuseable.QueueSubscription<ByteBuf> qs = store.framesSubscriber.qs;
    for (; ; ) {
      final long state = store.state;

      qs.clear();
      store.clearCache();

      if (isFinalized(state)) {
        return;
      }

      if (STATE.compareAndSet(store, state, state | FINALIZED_FLAG & ~MAX_WORK_IN_PROGRESS)) {
        store.disposed.tryEmitEmpty();
        store.framesSubscriber.onComplete();
        return;
      }
    }
  }

  static boolean isConnected(long state) {
    return (state & CONNECTED_FLAG) == CONNECTED_FLAG;
  }

  static boolean hasRemoteImpliedPositionChanged(long state) {
    return (state & REMOTE_IMPLIED_POSITION_CHANGED_FLAG) == REMOTE_IMPLIED_POSITION_CHANGED_FLAG;
  }

  static boolean hasPendingConnection(long state) {
    return (state & PENDING_CONNECTION_FLAG) == PENDING_CONNECTION_FLAG;
  }

  static boolean hasFrames(long state) {
    return (state & HAS_FRAME_FLAG) == HAS_FRAME_FLAG;
  }

  static boolean isTerminated(long state) {
    return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
  }

  static boolean isDisposed(long state) {
    return (state & DISPOSED_FLAG) == DISPOSED_FLAG;
  }

  static boolean isFinalized(long state) {
    return (state & FINALIZED_FLAG) == FINALIZED_FLAG;
  }

  static boolean isWorkInProgress(long state) {
    return (state & MAX_WORK_IN_PROGRESS) > 0;
  }
}
