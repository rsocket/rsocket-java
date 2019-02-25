package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.resume.ResumeAwareConnection;
import io.rsocket.resume.ResumeStateHolder;
import io.rsocket.util.DuplexConnectionProxy;
import io.rsocket.util.Function3;
import io.rsocket.util.KeepAliveData;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class KeepAliveConnection extends DuplexConnectionProxy implements ResumeAwareConnection {

  private final MonoProcessor<KeepAliveHandler> keepAliveHandlerReady = MonoProcessor.create();
  private final ByteBufAllocator allocator;
  private final Function<ByteBuf, Mono<KeepAliveData>> keepAliveData;
  private final Function3<ByteBufAllocator, Duration, Duration, KeepAliveHandler> keepAliveHandlerFactory;
  private final Consumer<Throwable> errorConsumer;
  private final UnicastProcessor<ByteBuf> keepAliveFrames = UnicastProcessor.create();
  private final EmitterProcessor<Long> lastReceivedPositions = EmitterProcessor.create();
  private volatile KeepAliveHandler keepAliveHandler;

  public static KeepAliveConnection ofClient(
      ByteBufAllocator allocator,
      DuplexConnection duplexConnection,
      Function<ByteBuf, Mono<KeepAliveData>> keepAliveData,
      Consumer<Throwable> errorConsumer) {

    return new KeepAliveConnection(
        allocator,
        duplexConnection,
        keepAliveData,
        KeepAliveHandler::ofClient,
        errorConsumer);
  }

  public static KeepAliveConnection ofServer(
      ByteBufAllocator allocator,
      DuplexConnection duplexConnection,
      Function<ByteBuf, Mono<KeepAliveData>> keepAliveData,
      Consumer<Throwable> errorConsumer) {

    return new KeepAliveConnection(
        allocator,
        duplexConnection,
        keepAliveData,
        KeepAliveHandler::ofServer,
        errorConsumer);
  }

  private KeepAliveConnection(
      ByteBufAllocator allocator,
      DuplexConnection duplexConnection,
      Function<ByteBuf, Mono<KeepAliveData>> keepAliveData,
      Function3<ByteBufAllocator, Duration, Duration, KeepAliveHandler> keepAliveHandlerFactory,
      Consumer<Throwable> errorConsumer) {
    super(duplexConnection);
    this.allocator = allocator;
    this.keepAliveData = keepAliveData;
    this.keepAliveHandlerFactory = keepAliveHandlerFactory;
    this.errorConsumer = errorConsumer;
    keepAliveHandlerReady.subscribe(this::startKeepAlives);
  }

  private void startKeepAlives(KeepAliveHandler keepAliveHandler) {
    this.keepAliveHandler = keepAliveHandler;
    send(keepAliveFrames).subscribe(null, err -> keepAliveHandler.dispose());

    keepAliveHandler
        .timeout()
        .subscribe(
            keepAlive -> {
              String message =
                  String.format("No keep-alive acks for %d ms", keepAlive.getTimeoutMillis());
              ConnectionErrorException err = new ConnectionErrorException(message);
              errorConsumer.accept(err);
              dispose();
            });
    keepAliveHandler.send().subscribe(keepAliveFrames::onNext);
    keepAliveHandler.start();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return super.send(
        Flux.from(frames)
            .doOnNext(
                f -> {
                  if (isStartFrame(f)) {
                    keepAliveHandler(keepAliveData.apply(f))
                        .subscribe(keepAliveHandlerReady);
                  }
                }));
  }

  @Override
  public Flux<ByteBuf> receive() {
    return super.receive()
        .doOnNext(
            f -> {
              if (isKeepAliveFrame(f)) {
                long receivedPos = keepAliveHandler.receive(f);
                if (isResumeRequested() && receivedPos > 0) {
                  lastReceivedPositions.onNext(receivedPos);
                }
              } else if (isStartFrame(f)) {
                keepAliveHandler(keepAliveData.apply(f)).subscribe(keepAliveHandlerReady);
              }
            });
  }

  @Override
  public Mono<Void> onClose() {
    return super.onClose()
        .then(Mono.fromRunnable(lastReceivedPositions::onComplete))
        .then(Mono.fromRunnable(() ->
            Optional
                .ofNullable(keepAliveHandlerReady.peek())
                .ifPresent(KeepAliveHandler::dispose))
        );
  }

  @Override
  public Flux<Long> receiveResumePositions(ResumeStateHolder resumeStateHolder) {
    return keepAliveHandlerReady
        .doOnNext(h -> h.resumeState(resumeStateHolder))
        .thenMany(lastReceivedPositions);
  }

  private boolean isResumeRequested() {
    return keepAliveHandler.hasResumeState();
  }

  private static boolean isStartFrame(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP
        || FrameHeaderFlyweight.frameType(frame) == FrameType.RESUME;
  }

  private static boolean isKeepAliveFrame(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.KEEPALIVE;
  }

  private Mono<KeepAliveHandler> keepAliveHandler(Mono<KeepAliveData> keepAliveData) {
    return keepAliveData.map(
        kad -> keepAliveHandlerFactory
            .apply(allocator, kad.getTickPeriod(), kad.getTimeout()));
  }
}
