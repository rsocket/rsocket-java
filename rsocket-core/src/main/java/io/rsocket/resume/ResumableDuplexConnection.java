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
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderFlyweight;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;

class ResumableDuplexConnection implements DuplexConnection, ResumeStateHolder {
  private static final Logger logger = LoggerFactory.getLogger(ResumableDuplexConnection.class);

  private final String tag;
  private final ResumedFramesCalculator resumedFramesCalculator;
  private final ResumableFramesStore resumableFramesStore;
  private final Duration resumeStreamTimeout;

  private final ReplayProcessor<DuplexConnection> connections = ReplayProcessor.create(1);
  private final EmitterProcessor<Throwable> connectionErrors = EmitterProcessor.create();
  private volatile ResumeAwareConnection curConnection;
  /*used instead of EmitterProcessor because its autocancel=false capability had no expected effect*/
  private final FluxProcessor<ByteBuf, ByteBuf> downStreamFrames = ReplayProcessor.create(0);
  private final FluxProcessor<ByteBuf, ByteBuf> resumeSaveFrames = EmitterProcessor.create();
  private final MonoProcessor<Void> resumeSaveCompleted = MonoProcessor.create();
  private final FluxProcessor<Object, Object> actions = UnicastProcessor.create().serialize();

  private final Mono<Void> framesSent;
  private final RequestListener downStreamRequestListener = new RequestListener();
  private final RequestListener resumeSaveStreamRequestListener = new RequestListener();
  private final UnicastProcessor<Flux<ByteBuf>> upstreams = UnicastProcessor.create();
  private final UpstreamFramesSubscriber upstreamSubscriber =
      new UpstreamFramesSubscriber(
          128,
          downStreamRequestListener.requests(),
          resumeSaveStreamRequestListener.requests(),
          actions::onNext);

  private volatile State state;
  private volatile Disposable resumedStreamDisposable = Disposables.disposed();
  private final AtomicBoolean disposed = new AtomicBoolean();

  ResumableDuplexConnection(
      String tag,
      ResumeAwareConnection duplexConnection,
      ResumedFramesCalculator resumedFramesCalculator,
      ResumableFramesStore resumableFramesStore,
      Duration resumeStreamTimeout) {
    this.tag = tag;
    this.resumedFramesCalculator = resumedFramesCalculator;
    this.resumableFramesStore = resumableFramesStore;
    this.resumeStreamTimeout = resumeStreamTimeout;

    resumableFramesStore
        .saveFrames(resumeSaveStreamRequestListener.apply(resumeSaveFrames))
        .subscribe(resumeSaveCompleted);

    upstreams.flatMap(Function.identity()).subscribe(upstreamSubscriber);

    framesSent =
        connections
            .switchMap(
                c -> {
                  logger.debug("Switching transport: {}", tag);
                  return c.send(downStreamRequestListener.apply(downStreamFrames))
                      .doFinally(
                          s ->
                              logger.debug(
                                  "{} Transport send completed: {}, {}", tag, s, c.toString()))
                      .onErrorResume(err -> Mono.never());
                })
            .then()
            .cache();

    dispatch(actions);
    reconnect(duplexConnection);
  }

  /*reconnected by session after error. After this downstream can receive frames,
   * but sending in suppressed until resume() is called*/
  public void reconnect(ResumeAwareConnection connection) {
    if (curConnection == null) {
      logger.debug("{} Resumable duplex connection started with connection: {}", tag, connection);
      state = State.CONNECTED;
      onNewConnection(connection);
      acceptRemoteResumePositions();
    } else {
      logger.debug(
          "{} Resumable duplex connection reconnected with connection: {}", tag, connection);
      /*race between sendFrame and doResumeStart may lead to ongoing upstream frames
      written before resume complete*/
      actions.onNext(new ResumeStart(connection));
    }
  }

  /*after receiving RESUME (Server) or RESUME_OK (Client)
  calculate and send resume frames */
  public void resume(
      long remotePos, long remoteImpliedPos, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
    /*race between sendFrame and doResume may lead to duplicate frames on resume store*/
    actions.onNext(new Resume(remotePos, remoteImpliedPos, resumeFrameSent));
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    return curConnection.sendOne(frame);
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return Mono.defer(
        () -> {
          upstreams.onNext(Flux.from(frames));
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
                        resumableFramesStore.resumableFrameReceived(f);
                      }
                    })
                .onErrorResume(err -> Mono.never()));
  }

  public long position() {
    return resumableFramesStore.framePosition();
  }

  @Override
  public long impliedPosition() {
    return resumableFramesStore.frameImpliedPosition();
  }

  @Override
  public void onImpliedPosition(long remoteImpliedPos) {
    logger.debug("Got remote position from keep-alive: {}", remoteImpliedPos);
    releaseFramesToPosition(remoteImpliedPos);
  }

  @Override
  public Mono<Void> onClose() {
    return Flux.merge(connections.last().flatMap(Closeable::onClose), resumeSaveCompleted).then();
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      logger.debug("Resumable connection disposed: {}, {}", tag, this);
      upstreams.onComplete();
      connections.onComplete();
      connectionErrors.onComplete();
      resumeSaveFrames.onComplete();
      curConnection.dispose();
      upstreamSubscriber.dispose();
      resumedStreamDisposable.dispose();
      resumableFramesStore.dispose();
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
    curConnection.acceptResumeState(this);
  }

  private void sendFrame(ByteBuf f) {
    /*resuming from store so no need to save again*/
    if (isResumableFrame(f) && state != State.RESUME) {
      resumeSaveFrames.onNext(f);
    }
    /*filter frames coming from upstream before actual resumption began,
     *  to preserve frames ordering*/
    if (state != State.RESUME_STARTED) {
      downStreamFrames.onNext(f);
    }
  }

  Flux<Throwable> connectionErrors() {
    return connectionErrors;
  }

  private void dispatch(Flux<?> f) {
    f.subscribe(
        o -> {
          if (o instanceof ByteBuf) {
            sendFrame((ByteBuf) o);
          } else {
            ((Runnable) o).run();
          }
        });
  }

  private void doResumeStart(ResumeAwareConnection connection) {
    state = State.RESUME_STARTED;
    resumedStreamDisposable.dispose();
    upstreamSubscriber.resumeStart();
    onNewConnection(connection);
  }

  private void doResume(
      long remotePosition,
      long remoteImpliedPosition,
      Function<Mono<Long>, Mono<Void>> sendResumeFrame) {
    long localPosition = position();
    long localImpliedPosition = impliedPosition();

    logger.debug(
        "Resumption start. Calculating implied pos using: {}",
        resumedFramesCalculator.getClass().getSimpleName());
    logger.debug(
        "Resumption states. local: [pos: {}, impliedPos: {}], remote: [pos: {}, impliedPos: {}]",
        localPosition,
        localImpliedPosition,
        remotePosition,
        remoteImpliedPosition);

    Mono<Long> res =
        resumedFramesCalculator.calculate(
            localPosition, localImpliedPosition,
            remotePosition, remoteImpliedPosition);
    Mono<Long> localImpliedPos =
        res.doOnSuccess(notUsed -> state = State.RESUME)
            .doOnSuccess(this::releaseFramesToPosition)
            .map(remoteImpliedPos -> localImpliedPosition);

    sendResumeFrame
        .apply(localImpliedPos)
        .then(
            streamResumedFrames(
                    resumableFramesStore
                        .resumeStream()
                        .timeout(resumeStreamTimeout)
                        .doFinally(s -> actions.onNext(new ResumeComplete())))
                .doOnError(err -> dispose()))
        .onErrorResume(err -> Mono.empty())
        .subscribe();
  }

  private void doResumeComplete() {
    logger.debug("Completing resumption");
    state = State.RESUME_COMPLETED;
    upstreamSubscriber.resumeComplete();
    acceptRemoteResumePositions();
  }

  private Mono<Void> streamResumedFrames(Flux<ByteBuf> frames) {
    return Mono.create(
        s -> {
          ResumeFramesSubscriber subscriber =
              new ResumeFramesSubscriber(
                  downStreamRequestListener.requests(), actions::onNext, s::error, s::success);
          s.onDispose(subscriber);
          resumedStreamDisposable = subscriber;
          frames.subscribe(subscriber);
        });
  }

  private void onNewConnection(ResumeAwareConnection connection) {
    curConnection = connection;
    connection.onClose().doFinally(v -> disconnect(connection)).subscribe();
    connections.onNext(connection);
  }

  private void disconnect(DuplexConnection connection) {
    /*do not report late disconnects on old connection if new one is available*/
    if (curConnection == connection && state.isActive()) {
      Throwable err = new ClosedChannelException();
      state = State.DISCONNECTED;
      logger.debug("{} Inner connection disconnected: {}", tag, err.getClass().getSimpleName());
      connectionErrors.onNext(err);
    }
  }

  /*remove frames confirmed by implied pos,
  set current pos accordingly*/
  private void releaseFramesToPosition(Long remoteImpliedPos) {
    resumableFramesStore.releaseFrames(remoteImpliedPos);
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

  class ResumeStart implements Runnable {
    private ResumeAwareConnection connection;

    public ResumeStart(ResumeAwareConnection connection) {
      this.connection = connection;
    }

    @Override
    public void run() {
      doResumeStart(connection);
    }
  }

  class Resume implements Runnable {
    private final long remotePos;
    private final long remoteImpliedPos;
    private final Function<Mono<Long>, Mono<Void>> resumeFrameSent;

    public Resume(
        long remotePos, long remoteImpliedPos, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
      this.remotePos = remotePos;
      this.remoteImpliedPos = remoteImpliedPos;
      this.resumeFrameSent = resumeFrameSent;
    }

    @Override
    public void run() {
      doResume(remotePos, remoteImpliedPos, resumeFrameSent);
    }
  }

  private class ResumeComplete implements Runnable {

    @Override
    public void run() {
      doResumeComplete();
    }
  }
}
