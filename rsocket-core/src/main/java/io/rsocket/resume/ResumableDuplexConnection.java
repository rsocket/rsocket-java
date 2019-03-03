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

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

class ResumableDuplexConnection implements DuplexConnection, ResumeStateHolder {
  private static final Logger logger = LoggerFactory.getLogger(ResumableDuplexConnection.class);

  private final String tag;
  private final ResumedFramesCalculator resumedFramesCalculator;
  private final ResumeStore resumeStore;
  private final Duration resumeStreamTimeout;

  private final ReplayProcessor<DuplexConnection> connections = ReplayProcessor.create(1);
  private final EmitterProcessor<Throwable> connectionErrors = EmitterProcessor.create();
  private volatile ResumeAwareConnection curConnection;
  /*used instead of EmitterProcessor because its autocancel=false capability had no expected effect*/
  private final FluxProcessor<ByteBuf, ByteBuf> downStreamFrames = ReplayProcessor.create(0);
  private final Mono<Void> framesSent;
  private final UnboundedProcessor<Object> actions = new UnboundedProcessor<>();
  private final UpstreamFramesSubscribers upsteamSubscribers = new UpstreamFramesSubscribers(128, actions::onNext);
  private final RequestListener requestListener = new RequestListener();
  private volatile State state;
  private volatile Disposable impliedPosDisposable = Disposables.disposed();
  private volatile Disposable resumedStreamDisposable = Disposables.disposed();
  private final AtomicBoolean disposed = new AtomicBoolean();

  ResumableDuplexConnection(
      String tag,
      ResumeAwareConnection duplexConnection,
      ResumedFramesCalculator resumedFramesCalculator,
      ResumeStore resumeStore,
      Duration resumeStreamTimeout) {
    this.tag = tag;
    this.resumedFramesCalculator = resumedFramesCalculator;
    this.resumeStore = resumeStore;
    this.resumeStreamTimeout = resumeStreamTimeout;

    framesSent =
        connections
            .switchMap(
                c -> {
                  logger.info("Switching transport: {}", tag);
                  return c.send(requestListener.apply(downStreamFrames))
                      .doFinally(
                          s ->
                              logger.info(
                                  "{} Transport send completed: {}, {}", tag, s, c.toString()))
                      .onErrorResume(err -> Mono.never());
                })
            .doOnError(err -> upsteamSubscribers.dispose())
            .onErrorResume(err -> Mono.empty())
            .then()
            .cache();

    Flux<Object> acts = actions.publish().autoConnect(3);
    acts.ofType(ByteBuf.class).subscribe(this::sendFrame);
    acts.ofType(Resume.class).subscribe(Resume::run);
    acts.ofType(ConnectionDisposed.class).subscribe(ConnectionDisposed::run);

    reconnect(duplexConnection);
  }

  /*reconnected by session after error. After this downstream can receive frames,
   * but sending in suppressed until resume() is called*/
  public void reconnect(ResumeAwareConnection connection) {
    if (curConnection == null) {
      logger.info("{} Resumable duplex connection started with connection: {}", tag, connection);
      state = State.CONNECTED;
    } else {
      logger.info("{} Resumable duplex connection reconnected with connection: {}", tag, connection);
      doResumeStart();
    }
    curConnection = connection;
    connection.onClose().doFinally(v -> disconnect(connection)).subscribe();
    connections.onNext(connection);
  }

  /*after receiving RESUME (Server) or RESUME_OK (Client)
  calculate and send resume frames */
  public void resume(
      ResumptionState peerResumptionState, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
    actions.onNext(new Resume(peerResumptionState, resumeFrameSent));
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    return curConnection.sendOne(frame);
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return Mono.defer(
        () -> {
          Flux.from(frames).subscribe(upsteamSubscribers.create(actions::onNext));
          return framesSent;
        });
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connections.switchMap(
        c ->
            c.receive()
                .doOnNext(
                    f -> {
                      if (isResumableFrame(f)) {
                        resumeStore.resumableFrameReceived();
                      }
                    })
                .onErrorResume(err -> Mono.never()));
  }

  @Override
  public long impliedPosition() {
    return resumeStore.frameImpliedPosition();
  }

  @Override
  public Mono<Void> onClose() {
    return connections.onErrorResume(err -> Mono.empty()).last().flatMap(Closeable::onClose);
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      logger.info("Resumable connection disposed: {}, {}", tag, this);
      actions.onNext(new ConnectionDisposed());
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
    resumeStore.dispose();
  }

  private void acceptRemoteResumePositions() {
    impliedPosDisposable.dispose();
    impliedPosDisposable =
        curConnection.receiveResumePositions(this)
            .doOnNext(l -> logger.info("Got remote position from keep-alive: {}", l))
            .subscribe(this::releaseFramesToPosition);
  }

  private void sendFrame(ByteBuf f) {
    /*resuming from store so no need to save again*/
    if (isResumableFrame(f) && state != State.RESUME) {
      resumeStore.saveFrame(f);
    }
    /*filter frames coming from upstream before actual resumption began,
    *  to preserve frames ordering*/
    if (state != State.RESUME_STARTED) {
      downStreamFrames.onNext(f);
    }
  }

  ResumptionState state() {
    return new ResumptionState(
        resumeStore.framePosition(),
        resumeStore.frameImpliedPosition());
  }

  Flux<Throwable> connectionErrors() {
    return connectionErrors;
  }

  private void doResumeStart() {
    state = State.RESUME_STARTED;
    resumedStreamDisposable.dispose();
    upsteamSubscribers.resumeStart();
  }

  private void doResume(
      ResumptionState peerResumptionState,
      Function<Mono<Long>, Mono<Void>> sendResumeFrame) {
    ResumptionState localResumptionState = state();

    logger.info(
        "Resumption start. Calculating implied pos using: {}",
        resumedFramesCalculator.getClass().getSimpleName());
    logger.info(
        "Resumption states. local: {}, remote: {}", localResumptionState, peerResumptionState);

    Mono<Long> res = resumedFramesCalculator.calculate(localResumptionState, peerResumptionState);
    Mono<Long> localImpliedPos =
        res.doOnSuccess(notUsed -> state = State.RESUME)
            .doOnSuccess(this::releaseFramesToPosition)
            .map(remoteImpliedPos -> localResumptionState.impliedPosition());

    sendResumeFrame
        .apply(localImpliedPos)
        .then(streamResumedFrames(resumeStore.resumeStream().timeout(resumeStreamTimeout)
            .doFinally(s -> doResumeComplete()))
                .doOnError(err -> dispose())
        )
        .onErrorResume(err -> Mono.empty())
        .subscribe();
  }

  private void doResumeComplete() {
    logger.info("Completing resumption");
    state = State.RESUME_COMPLETED;
    upsteamSubscribers.resumeComplete();
    acceptRemoteResumePositions();
  }

  private Mono<Void> streamResumedFrames(Flux<ByteBuf> frames) {
    return Mono.create(s -> {
      ResumeFramesSubscriber subscriber = new ResumeFramesSubscriber(
          requestListener.requests(),
          actions::onNext,
          s::error,
          s::success);
      s.onDispose(subscriber);
      resumedStreamDisposable = subscriber;
      frames.subscribe(subscriber);
    });
  }

  private void disconnect(DuplexConnection connection) {
    /*do not report late disconnects on old connection if new one is available*/
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
  private void releaseFramesToPosition(Long remoteImpliedPos) {
    resumeStore.releaseFrames(remoteImpliedPos);
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
    RESUME_STARTED(true),
    RESUME(true),
    RESUME_COMPLETED(true),
    DISCONNECTED(false);

    private final boolean active;

    State(boolean active) {
      this.active = active;
    }

    public boolean isActive() {
      return active;
    }
  }

  class Resume implements Runnable {
    private final ResumptionState peerResumptionState;
    private final Function<Mono<Long>, Mono<Void>> resumeFrameSent;

    public Resume(
        ResumptionState peerResumptionState, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
      this.peerResumptionState = peerResumptionState;
      this.resumeFrameSent = resumeFrameSent;
    }

    @Override
    public void run() {
      doResume(peerResumptionState, resumeFrameSent);
    }
  }

  class ConnectionDisposed implements Runnable {

    @Override
    public void run() {
      terminate();
    }
  }
}
