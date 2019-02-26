package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderFlyweight;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;

import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class ResumableDuplexConnection implements DuplexConnection, ResumeStateHolder {
  private static final Logger logger = LoggerFactory.getLogger(ResumableDuplexConnection.class);

  private final long cachedFramesLimit;
  private final ReplayProcessor<DuplexConnection> connections = ReplayProcessor.create(1);
  private final EmitterProcessor<Throwable> connectionErrors = EmitterProcessor.create();
  private volatile ResumeAwareConnection curConnection;
  private final AtomicBoolean disposed = new AtomicBoolean();
  private final Queue<ByteBuf> cachedFrames = new ConcurrentLinkedQueue<>();
  private final AtomicInteger cachedFramesSize = new AtomicInteger();
  private final AtomicLong position = new AtomicLong();
  private final AtomicLong impliedPosition = new AtomicLong();
  private volatile Disposable impliedPosDisposable = Disposables.disposed();
  private volatile State state;
  private final String tag;
  private final ResumedFramesCalculator resumedFramesCalculator;
  private final FluxProcessor<Object, Object> actions = EmitterProcessor.create().serialize();
  private final FluxProcessor<ByteBuf, ByteBuf> framesAcceptor =
      ReplayProcessor.<ByteBuf>create(0).serialize();
  private final Disposable.Composite sendPublisherDisposables = Disposables.composite();
  private final Mono<Void> framesSent;

  ResumableDuplexConnection(
      String tag,
      ResumeAwareConnection duplexConnection,
      ResumedFramesCalculator resumedFramesCalculator,
      long cachedFramesLimit) {
    this.tag = tag;
    this.resumedFramesCalculator = resumedFramesCalculator;
    this.cachedFramesLimit = cachedFramesLimit;

    framesSent =
        connections
            .switchMap(
                c -> {
                  logger.info("Switching transport: {}", tag);
                  return c.send(framesAcceptor)
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

    actions.ofType(ByteBuf.class).subscribe(this::sendFrame);
    actions.ofType(ResumeStart.class).subscribe(ResumeStart::run);
    actions.ofType(ResumeComplete.class).subscribe(ResumeComplete::run);

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
      Throwable err = connectionErrors.getError();
      if (err == null) {
        connections.onComplete();
      } else {
        connections.onError(err);
      }
      connectionErrors.onComplete();
      curConnection.dispose();

      impliedPosDisposable.dispose();

      cachedFramesSize.set(0);
      cachedFrames.forEach(byteBuf -> byteBuf.release(byteBuf.refCnt()));
      cachedFrames.clear();
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
      if (state.isResumed()) {
        framesAcceptor.onNext(f);
      }
    } else {
      logger.info("{} Send non-resumable frame, connection: {}", tag, curConnection);
      framesAcceptor.onNext(f);
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
    Iterator<ByteBuf> iterator = cachedFrames.iterator();
    while (iterator.hasNext()) {
      ByteBuf content = iterator.next();
      if (removedCount < toRemoveCount) {
        iterator.remove();
        removedCount++;
      } else {
        content.retain();
      }
      framesAcceptor.onNext(content.resetReaderIndex());
    }
    state = State.RESUMED;
    acceptRemoteResumePositions();
  }

  private void disconnect(DuplexConnection connection) {
    Throwable err = new ClosedChannelException();
    if (curConnection == connection && state.isActive()) {
      state = State.DISCONNECTED;
      logger.info(
          "{} Inner connection disconnected: {}, {}",
          tag,
          err.getClass().getSimpleName(),
          err.getMessage());
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
}
