package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.internal.UnboundedProcessor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;
import reactor.util.concurrent.Queues;

import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class ResumableDuplexConnection implements DuplexConnection, ResumeStateHolder {
  private static final Logger logger = LoggerFactory.getLogger(ResumableDuplexConnection.class);

  private final long cachedFramesLimit;
  private final long cachedFramesCap;
  private final ReplayProcessor<DuplexConnection> connections = ReplayProcessor.create(1);
  private final EmitterProcessor<Throwable> connectionErrors = EmitterProcessor.create();
  private volatile ResumeAwareConnection curConnection;
  private final AtomicBoolean disposed = new AtomicBoolean();
  private final Queue<ByteBuf> cachedFrames;
  private final AtomicInteger cachedFramesSize = new AtomicInteger();
  private final AtomicLong position = new AtomicLong();
  private final AtomicLong impliedPosition = new AtomicLong();
  private volatile Disposable impliedPosDisposable = Disposables.disposed();
  private volatile State state;
  private final String tag;
  private final ResumedFramesCalculator resumedFramesCalculator;
  private final UnboundedProcessor<Object> actions = new UnboundedProcessor<>();
  /*used instead of EmitterProcessor because its autocancel=false capability had no expected effect*/
  private final FluxProcessor<ByteBuf, ByteBuf> sentFrames = ReplayProcessor.create(0);
  private final Disposable.Composite sendPublisherDisposables = Disposables.composite();
  private final Mono<Void> framesSent;

  ResumableDuplexConnection(
      String tag,
      ResumeAwareConnection duplexConnection,
      ResumedFramesCalculator resumedFramesCalculator,
      int cachedFramesLimit,
      int cachedFramesCap) {
    this.tag = tag;
    this.resumedFramesCalculator = resumedFramesCalculator;
    this.cachedFramesLimit = cachedFramesLimit;
    this.cachedFramesCap = cachedFramesCap;
    this.cachedFrames = cachedFramesQueue(cachedFramesLimit);

    framesSent =
        connections
            .switchMap(
                c -> {
                  logger.info("Switching transport: {}", tag);
                  return c.send(sentFrames)
                      .doFinally(
                          s ->
                              logger.info(
                                  "{} Transport send completed: {}, {}", tag, s, c.toString()))
                      .onErrorResume(err -> Mono.never());
                })
            .doOnError(err -> sendPublisherDisposables.dispose())
            .onErrorResume(err -> Mono.empty())
            .then()
            .cache();

    Flux<Object> acts = actions.publish().autoConnect(4);
    acts.ofType(ByteBuf.class).subscribe(this::sendFrame);
    acts.ofType(ResumeStart.class).subscribe(ResumeStart::run);
    acts.ofType(ResumeComplete.class).subscribe(ResumeComplete::run);
    acts.ofType(Disposed.class).subscribe(Disposed::run);

    reconnect(duplexConnection);
  }

  /*reconnected by session after error. After this downstream can receive frames,
   * but sending in suppressed until resume() is called*/
  public void reconnect(ResumeAwareConnection connection) {
    logger.info("{} Resumable duplex connection reconnected with connection: {}", tag, connection);
    state = curConnection == null ? State.RESUMED : State.CONNECTED;
    curConnection = connection;
    connection.onClose().doFinally(v -> disconnect(connection)).subscribe();
    connections.onNext(connection);
  }

  /*after receiving RESUME (Server) or RESUME_OK (Client)
  calculate and send resume frames */
  public void resume(
      ResumptionState peerResumptionState, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
    actions.onNext(new ResumeStart(peerResumptionState, resumeFrameSent));
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    return curConnection.sendOne(frame);
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return Mono.defer(
        () -> {
          sendPublisherDisposables.add(Flux.from(frames).subscribe(actions::onNext));
          return framesSent;
        });
  }

  @Override
  public long impliedPosition() {
    return impliedPosition.get();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connections.switchMap(
        c ->
            c.receive()
                .doOnNext(
                    f -> {
                      if (isResumableFrame(f)) {
                        impliedPosition.incrementAndGet();
                      }
                    })
                .onErrorResume(err -> Mono.never()));
  }

  @Override
  public Mono<Void> onClose() {
    return connections.onErrorResume(err -> Mono.empty()).last().flatMap(Closeable::onClose);
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      logger.info("Resumable connection disposed: {}, {}", tag, this);
      actions.onNext(new Disposed());
    }
  }

  @Override
  public double availability() {
    return curConnection.availability();
  }

  @Override
  public boolean isDisposed() {
    return disposed.get();
  }

  private void terminate() {
    connections.onComplete();
    connectionErrors.onComplete();
    curConnection.dispose();

    impliedPosDisposable.dispose();

    cachedFramesSize.set(0);
    releaseCachedFrames();
  }

  private void releaseCachedFrames() {
    ByteBuf frame = cachedFrames.poll();
    while (frame != null) {
      frame.release(frame.refCnt());
      frame = cachedFrames.poll();
    }
  }

  private void acceptRemoteResumePositions() {
    impliedPosDisposable.dispose();
    impliedPosDisposable =
        curConnection.receiveResumePositions(this)
            .doOnNext(l -> logger.info("Got remote position from keep-alive: {}", l))
            .subscribe(this::removeCachedPrefix);
  }

  private void sendFrame(ByteBuf f) {
    if (isResumableFrame(f)) {
      int size = saveFrame(f);
      /*frames should not be removed while resumption is happening*/
      if (size > cachedFramesLimit && !state.isResuming()) {
        releaseFrame();
      }
      if (size > cachedFramesCap) {
        terminate();
      }
      if (state.isResumed()) {
        sentFrames.onNext(f);
      }
    } else {
      logger.info("{} Send non-resumable frame, connection: {}", tag, curConnection);
      sentFrames.onNext(f);
    }
  }

  ResumptionState state() {
    return new ResumptionState(position.get(), impliedPosition.get());
  }

  Flux<Throwable> connectionErrors() {
    return connectionErrors;
  }

  private void resumeStart(
      ResumptionState peerResumptionState, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
    state = State.RESUMING;
    ResumptionState localResumptionState = state();

    logger.info(
        "Resumption start. Calculating implied pos using: {}",
        resumedFramesCalculator.getClass().getSimpleName());
    logger.info(
        "Resumption states. local: {}, remote: {}", localResumptionState, peerResumptionState);

    Mono<Long> res = resumedFramesCalculator.calculate(localResumptionState, peerResumptionState);
    Mono<Long> clearedStorage =
        res.doOnSuccess(this::removeCachedPrefix)
            .map(remoteImpliedPos -> localResumptionState.impliedPosition());

    resumeFrameSent
        .apply(clearedStorage)
        .doOnSuccess(v -> actions.onNext(new ResumeComplete()))
        .onErrorResume(err -> Mono.empty())
        .subscribe();
  }

  private void resumeComplete() {
    int size = cachedFramesSize.get();
    logger.info("Completing resumption. Cached frames size: {}", size);
    long toRemoveCount = Math.max(0, size - cachedFramesLimit);
    long removedCount = 0;
    long processedCount = 0;
    /*SpscQueue does not have iterator*/
    while (processedCount < size) {
      ByteBuf frame = cachedFrames.poll();
      if (removedCount < toRemoveCount) {
        removedCount++;
      } else {
        cachedFrames.offer(frame.retain());
      }
      sentFrames.onNext(frame);
      processedCount++;
    }
    state = State.RESUMED;
    acceptRemoteResumePositions();
  }

  private void disconnect(DuplexConnection connection) {
    if (curConnection == connection && state.isActive()) {
      Throwable err = new ClosedChannelException();
      state = State.DISCONNECTED;
      logger.info(
          "{} Inner connection disconnected: {}",
          tag,
          err.getClass().getSimpleName());
      connectionErrors.onNext(err);
    }
  }

  /*remove frames confirmed by implied pos,
  set current pos accordingly*/
  private void removeCachedPrefix(Long remoteImpliedPos) {
    long pos = position.get();
    long removeCount = remoteImpliedPos - pos;
    logger.info("Removing frames from current pos: {} to implied pos: {}", pos, remoteImpliedPos);
    if (removeCount > 0) {
      for (int i = 0; i < removeCount; i++) {
        releaseFrame();
      }
      position.set(remoteImpliedPos);
    }
  }

  private void releaseFrame() {
    ByteBuf content = cachedFrames.poll();
    cachedFramesSize.decrementAndGet();
    content.release(content.refCnt());
    position.incrementAndGet();
  }

  private int saveFrame(ByteBuf f) {
    cachedFrames.offer(f.retain());
    return cachedFramesSize.incrementAndGet();
  }

  static boolean isResumableFrame(ByteBuf frame) {
    switch (FrameHeaderFlyweight.frameType(frame)) {
      case REQUEST_CHANNEL:
      case REQUEST_STREAM:
      case REQUEST_RESPONSE:
      case REQUEST_FNF:
      case REQUEST_N:
      case CANCEL:
      case ERROR:
      case NEXT:
      case NEXT_COMPLETE:
        return true;
      default:
        return false;
    }
  }

  private static Queue<ByteBuf> cachedFramesQueue(int framesLimit) {
    return Queues.<ByteBuf>unbounded(framesLimit / 10).get();
  }

  private enum State {
    CONNECTED(true),

    RESUMING(true) {
      @Override
      public boolean isResuming() {
        return true;
      }
    },

    RESUMED(true) {
      @Override
      public boolean isResumed() {
        return true;
      }
    },

    DISCONNECTED(false);

    private final boolean active;

    State(boolean active) {
      this.active = active;
    }

    public boolean isResumed() {
      return false;
    }

    public boolean isResuming() {
      return false;
    }

    public boolean isActive() {
      return active;
    }
  }

  class ResumeStart implements Runnable {
    private final ResumptionState peerResumptionState;
    private final Function<Mono<Long>, Mono<Void>> resumeFrameSent;

    public ResumeStart(
        ResumptionState peerResumptionState, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
      this.peerResumptionState = peerResumptionState;
      this.resumeFrameSent = resumeFrameSent;
    }

    @Override
    public void run() {
      resumeStart(peerResumptionState, resumeFrameSent);
    }
  }

  class ResumeComplete implements Runnable {

    @Override
    public void run() {
      resumeComplete();
    }
  }

  class Disposed implements Runnable {

    @Override
    public void run() {
      terminate();
    }
  }
}
