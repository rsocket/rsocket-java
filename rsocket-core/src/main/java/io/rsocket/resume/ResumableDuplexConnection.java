/// *
// * Copyright 2015-2019 the original author or authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package io.rsocket.resume;
//
// import io.netty.buffer.ByteBuf;
// import io.netty.buffer.ByteBufAllocator;
// import io.rsocket.Closeable;
// import io.rsocket.DuplexConnection;
// import io.rsocket.frame.FrameHeaderCodec;
// import java.nio.channels.ClosedChannelException;
// import java.time.Duration;
// import java.util.Queue;
// import java.util.concurrent.atomic.AtomicBoolean;
// import java.util.concurrent.atomic.AtomicInteger;
// import java.util.function.Function;
// import org.reactivestreams.Publisher;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import reactor.core.Disposable;
// import reactor.core.Disposables;
// import reactor.core.publisher.*;
// import reactor.util.concurrent.Queues;
//
// public class ResumableDuplexConnection implements DuplexConnection, ResumeStateHolder {
//  private static final Logger logger = LoggerFactory.getLogger(ResumableDuplexConnection.class);
//  private static final Throwable closedChannelException = new ClosedChannelException();
//
//  private final String tag;
//  private final ResumableFramesStore resumableFramesStore;
//  private final Duration resumeStreamTimeout;
//  private final boolean cleanupOnKeepAlive;
//
//  private final ReplayProcessor<DuplexConnection> connections = ReplayProcessor.create(1);
//  private final EmitterProcessor<Throwable> connectionErrors = EmitterProcessor.create();
//  private volatile DuplexConnection curConnection;
//  /*used instead of EmitterProcessor because its autocancel=false capability had no expected
// effect*/
//  private final FluxProcessor<ByteBuf, ByteBuf> downStreamFrames = ReplayProcessor.create(0);
//  private final FluxProcessor<ByteBuf, ByteBuf> resumeSaveFrames = EmitterProcessor.create();
//  private final MonoProcessor<Void> resumeSaveCompleted = MonoProcessor.create();
//  private final Queue<Object> actions = Queues.unboundedMultiproducer().get();
//  private final AtomicInteger actionsWip = new AtomicInteger();
//  private final AtomicBoolean disposed = new AtomicBoolean();
//
//  private final Mono<Void> framesSent;
//  private final RequestListener downStreamRequestListener = new RequestListener();
//  private final RequestListener resumeSaveStreamRequestListener = new RequestListener();
//  private final UnicastProcessor<Flux<ByteBuf>> upstreams = UnicastProcessor.create();
//  private final UpstreamFramesSubscriber upstreamSubscriber =
//      new UpstreamFramesSubscriber(
//          Queues.SMALL_BUFFER_SIZE,
//          downStreamRequestListener.requests(),
//          resumeSaveStreamRequestListener.requests(),
//          this::dispatch);
//
//  private volatile Runnable onResume;
//  private volatile Runnable onDisconnect;
//  private volatile int state;
//  private volatile Disposable resumedStreamDisposable = Disposables.disposed();
//
//  public ResumableDuplexConnection(
//      String tag,
//      DuplexConnection duplexConnection,
//      ResumableFramesStore resumableFramesStore,
//      Duration resumeStreamTimeout,
//      boolean cleanupOnKeepAlive) {
//    this.tag = tag;
//    this.resumableFramesStore = resumableFramesStore;
//    this.resumeStreamTimeout = resumeStreamTimeout;
//    this.cleanupOnKeepAlive = cleanupOnKeepAlive;
//
//    resumableFramesStore
//        .saveFrames(resumeSaveStreamRequestListener.apply(resumeSaveFrames))
//        .subscribe(resumeSaveCompleted);
//
//    upstreams.flatMap(Function.identity()).subscribe(upstreamSubscriber);
//
//    framesSent =
//        connections
//            .switchMap(
//                c -> {
//                  logger.debug("Switching transport: {}", tag);
//                  return c.send(downStreamRequestListener.apply(downStreamFrames))
//                      .doFinally(
//                          s ->
//                              logger.debug(
//                                  "{} Transport send completed: {}, {}", tag, s, c.toString()))
//                      .onErrorResume(err -> Mono.never());
//                })
//            .then()
//            .cache();
//
//    reconnect(duplexConnection);
//  }
//
//  @Override
//  public ByteBufAllocator alloc() {
//    return curConnection.alloc();
//  }
//
//  public void disconnect() {
//    DuplexConnection c = this.curConnection;
//    if (c != null) {
//      disconnect(c);
//    }
//  }
//
//  public void onDisconnect(Runnable onDisconnectAction) {
//    this.onDisconnect = onDisconnectAction;
//  }
//
//  public void onResume(Runnable onResumeAction) {
//    this.onResume = onResumeAction;
//  }
//
//  /*reconnected by session after error. After this downstream can receive frames,
//   * but sending in suppressed until resume() is called*/
//  public void reconnect(DuplexConnection connection) {
//    if (curConnection == null) {
//      logger.debug("{} Resumable duplex connection started with connection: {}", tag, connection);
//      state = State.CONNECTED;
//      onNewConnection(connection);
//    } else {
//      logger.debug(
//          "{} Resumable duplex connection reconnected with connection: {}", tag, connection);
//      /*race between sendFrame and doResumeStart may lead to ongoing upstream frames
//      written before resume complete*/
//      dispatch(new ResumeStart(connection));
//    }
//  }
//
//  /*after receiving RESUME (Server) or RESUME_OK (Client)
//  calculate and send resume frames */
//  public void resume(
//      long remotePos, long remoteImpliedPos, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
//    /*race between sendFrame and doResume may lead to duplicate frames on resume store*/
//    dispatch(new Resume(remotePos, remoteImpliedPos, resumeFrameSent));
//  }
//
//  @Override
//  public Mono<Void> sendOne(ByteBuf frame) {
//    return curConnection.sendOne(frame);
//  }
//
//  @Override
//  public Mono<Void> send(Publisher<ByteBuf> frames) {
//    upstreams.onNext(Flux.from(frames));
//    return framesSent;
//  }
//
//  @Override
//  public Flux<ByteBuf> receive() {
//    return connections.switchMap(
//        c ->
//            c.receive()
//                .doOnNext(
//                    f -> {
//                      if (isResumableFrame(f)) {
//                        resumableFramesStore.resumableFrameReceived(f);
//                      }
//                    })
//                .onErrorResume(err -> Mono.never()));
//  }
//
//  public long position() {
//    return resumableFramesStore.framePosition();
//  }
//
//  @Override
//  public long impliedPosition() {
//    return resumableFramesStore.frameImpliedPosition();
//  }
//
//  @Override
//  public void onImpliedPosition(long remoteImpliedPos) {
//    logger.debug("Got remote position from keep-alive: {}", remoteImpliedPos);
//    if (cleanupOnKeepAlive) {
//      dispatch(new ReleaseFrames(remoteImpliedPos));
//    }
//  }
//
//  @Override
//  public Mono<Void> onClose() {
//    return Flux.merge(connections.last().flatMap(Closeable::onClose), resumeSaveCompleted).then();
//  }
//
//  @Override
//  public void dispose() {
//    if (disposed.compareAndSet(false, true)) {
//      logger.debug("Resumable connection disposed: {}, {}", tag, this);
//      upstreams.onComplete();
//      connections.onComplete();
//      connectionErrors.onComplete();
//      resumeSaveFrames.onComplete();
//      curConnection.dispose();
//      upstreamSubscriber.dispose();
//      resumedStreamDisposable.dispose();
//      resumableFramesStore.dispose();
//    }
//  }
//
//  @Override
//  public double availability() {
//    return curConnection.availability();
//  }
//
//  @Override
//  public boolean isDisposed() {
//    return disposed.get();
//  }
//
//  private void sendFrame(ByteBuf f) {
//    if (disposed.get()) {
//      f.release();
//      return;
//    }
//    /*resuming from store so no need to save again*/
//    if (state != State.RESUME && isResumableFrame(f)) {
//      resumeSaveFrames.onNext(f);
//    }
//    /*filter frames coming from upstream before actual resumption began,
//     *  to preserve frames ordering*/
//    if (state != State.RESUME_STARTED) {
//      downStreamFrames.onNext(f);
//    }
//  }
//
//  Flux<Throwable> connectionErrors() {
//    return connectionErrors;
//  }
//
//  private void dispatch(Object action) {
//    actions.offer(action);
//    if (actionsWip.getAndIncrement() == 0) {
//      do {
//        Object a = actions.poll();
//        if (a instanceof ByteBuf) {
//          sendFrame((ByteBuf) a);
//        } else {
//          ((Runnable) a).run();
//        }
//      } while (actionsWip.decrementAndGet() != 0);
//    }
//  }
//
//  private void doResumeStart(DuplexConnection connection) {
//    state = State.RESUME_STARTED;
//    resumedStreamDisposable.dispose();
//    upstreamSubscriber.resumeStart();
//    onNewConnection(connection);
//  }
//
//  private void doResume(
//      long remotePosition,
//      long remoteImpliedPosition,
//      Function<Mono<Long>, Mono<Void>> sendResumeFrame) {
//    long localPosition = position();
//    long localImpliedPosition = impliedPosition();
//
//    logger.debug("Resumption start");
//    logger.debug(
//        "Resumption states. local: [pos: {}, impliedPos: {}], remote: [pos: {}, impliedPos: {}]",
//        localPosition,
//        localImpliedPosition,
//        remotePosition,
//        remoteImpliedPosition);
//
//    long remoteImpliedPos =
//        calculateRemoteImpliedPos(
//            localPosition, localImpliedPosition,
//            remotePosition, remoteImpliedPosition);
//
//    Mono<Long> impliedPositionOrError;
//    if (remoteImpliedPos >= 0) {
//      state = State.RESUME;
//      releaseFramesToPosition(remoteImpliedPos);
//      impliedPositionOrError = Mono.just(localImpliedPosition);
//    } else {
//      impliedPositionOrError =
//          Mono.error(
//              new ResumeStateException(
//                  localPosition, localImpliedPosition,
//                  remotePosition, remoteImpliedPosition));
//    }
//
//    sendResumeFrame
//        .apply(impliedPositionOrError)
//        .doOnSuccess(
//            v -> {
//              Runnable r = this.onResume;
//              if (r != null) {
//                r.run();
//              }
//            })
//        .then(
//            streamResumedFrames(
//                    resumableFramesStore
//                        .resumeStream()
//                        .timeout(resumeStreamTimeout)
//                        .doFinally(s -> dispatch(new ResumeComplete())))
//                .doOnError(err -> dispose()))
//        .onErrorResume(err -> Mono.empty())
//        .subscribe();
//  }
//
//  static long calculateRemoteImpliedPos(
//      long pos, long impliedPos, long remotePos, long remoteImpliedPos) {
//    if (remotePos <= impliedPos && pos <= remoteImpliedPos) {
//      return remoteImpliedPos;
//    } else {
//      return -1L;
//    }
//  }
//
//  private void doResumeComplete() {
//    logger.debug("Completing resumption");
//    state = State.RESUME_COMPLETED;
//    upstreamSubscriber.resumeComplete();
//  }
//
//  private Mono<Void> streamResumedFrames(Flux<ByteBuf> frames) {
//    return Mono.create(
//        s -> {
//          ResumeFramesSubscriber subscriber =
//              new ResumeFramesSubscriber(
//                  downStreamRequestListener.requests(), this::dispatch, s::error, s::success);
//          s.onDispose(subscriber);
//          resumedStreamDisposable = subscriber;
//          frames.subscribe(subscriber);
//        });
//  }
//
//  private void onNewConnection(DuplexConnection connection) {
//    curConnection = connection;
//    connection.onClose().doFinally(v -> disconnect(connection)).subscribe();
//    connections.onNext(connection);
//  }
//
//  private void disconnect(DuplexConnection connection) {
//    /*do not report late disconnects on old connection if new one is available*/
//    if (curConnection == connection && state != State.DISCONNECTED) {
//      connection.dispose();
//      state = State.DISCONNECTED;
//      logger.debug(
//          "{} Inner connection disconnected: {}",
//          tag,
//          closedChannelException.getClass().getSimpleName());
//      connectionErrors.onNext(closedChannelException);
//      Runnable r = this.onDisconnect;
//      if (r != null) {
//        r.run();
//      }
//    }
//  }
//
//  /*remove frames confirmed by implied pos,
//  set current pos accordingly*/
//  private void releaseFramesToPosition(long remoteImpliedPos) {
//    resumableFramesStore.releaseFrames(remoteImpliedPos);
//  }
//
//  static boolean isResumableFrame(ByteBuf frame) {
//    switch (FrameHeaderCodec.nativeFrameType(frame)) {
//      case REQUEST_CHANNEL:
//      case REQUEST_STREAM:
//      case REQUEST_RESPONSE:
//      case REQUEST_FNF:
//      case REQUEST_N:
//      case CANCEL:
//      case ERROR:
//      case PAYLOAD:
//        return true;
//      default:
//        return false;
//    }
//  }
//
//  static class State {
//    static int CONNECTED = 0;
//    static int RESUME_STARTED = 1;
//    static int RESUME = 2;
//    static int RESUME_COMPLETED = 3;
//    static int DISCONNECTED = 4;
//  }
//
//  class ResumeStart implements Runnable {
//    private final DuplexConnection connection;
//
//    public ResumeStart(DuplexConnection connection) {
//      this.connection = connection;
//    }
//
//    @Override
//    public void run() {
//      doResumeStart(connection);
//    }
//  }
//
//  class Resume implements Runnable {
//    private final long remotePos;
//    private final long remoteImpliedPos;
//    private final Function<Mono<Long>, Mono<Void>> resumeFrameSent;
//
//    public Resume(
//        long remotePos, long remoteImpliedPos, Function<Mono<Long>, Mono<Void>> resumeFrameSent) {
//      this.remotePos = remotePos;
//      this.remoteImpliedPos = remoteImpliedPos;
//      this.resumeFrameSent = resumeFrameSent;
//    }
//
//    @Override
//    public void run() {
//      doResume(remotePos, remoteImpliedPos, resumeFrameSent);
//    }
//  }
//
//  private class ResumeComplete implements Runnable {
//
//    @Override
//    public void run() {
//      doResumeComplete();
//    }
//  }
//
//  private class ReleaseFrames implements Runnable {
//    private final long remoteImpliedPos;
//
//    public ReleaseFrames(long remoteImpliedPos) {
//      this.remoteImpliedPos = remoteImpliedPos;
//    }
//
//    @Override
//    public void run() {
//      releaseFramesToPosition(remoteImpliedPos);
//    }
//  }
// }
