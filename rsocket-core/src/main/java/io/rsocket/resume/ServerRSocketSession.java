package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.ResumeOkFrameFlyweight;
import io.rsocket.util.KeepAliveData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.util.Objects;

public class ServerRSocketSession implements RSocketSession<ResumeAwareConnection> {
  private static final Logger logger = LoggerFactory.getLogger(ServerRSocketSession.class);

  private final ResumableDuplexConnection resumableConnection;
  private final FluxProcessor<ResumeAwareConnection, ResumeAwareConnection> newConnections =
      ReplayProcessor.create(0);
  private ByteBufAllocator allocator;
  private final KeepAliveData keepAliveData;
  private final ResumeToken resumeToken;

  public ServerRSocketSession(
      ByteBufAllocator allocator,
      ResumeAwareConnection duplexConnection,
      ServerResumeConfiguration config,
      KeepAliveData keepAliveData,
      ResumeToken resumeToken) {
    this.allocator = Objects.requireNonNull(allocator);
    this.keepAliveData = Objects.requireNonNull(keepAliveData);
    this.resumeToken = Objects.requireNonNull(resumeToken);
    this.resumableConnection =
        new ResumableDuplexConnection(
            "server", duplexConnection, ResumedFramesCalculator.ofServer, config.cacheSizeFrames());

    Mono<ResumeAwareConnection> timeout =
        resumableConnection
            .connectionErrors()
            .flatMap(
                err -> {
                  logger.info("Starting session timeout due to error: {}", err);
                  return newConnections
                      .next()
                      .doOnNext(c -> logger.info("Connection after error: {}", c))
                      .timeout(config.sessionDuration());
                })
            .then()
            .cast(ResumeAwareConnection.class);

    newConnections
        .mergeWith(timeout)
        .subscribe(
            connection -> {
              reconnect(connection);
              logger.info("Server ResumableConnection reconnected: {}", connection);
            },
            err -> {
              logger.info("Server ResumableConnection reconnect timeout");
              resumableConnection.dispose();
            });
  }

  @Override
  public ServerRSocketSession continueWith(ResumeAwareConnection newConnection) {
    logger.info("Server continued with connection: {}", newConnection);
    newConnections.onNext(newConnection);
    return this;
  }

  @Override
  public ServerRSocketSession resumeWith(ByteBuf resumeFrame) {
    logger.info("Resume FRAME received");
    resumableConnection.resume(
        stateFromFrame(resumeFrame),
        pos ->
            pos.flatMap(impliedPos ->
                sendFrame(ResumeOkFrameFlyweight.encode(allocator, impliedPos)))
                .onErrorResume(
                    err ->
                        sendFrame(ErrorFrameFlyweight.encode(
                            allocator,
                            0,
                            errorFrameThrowable(err)))
                            .then(Mono.fromRunnable(resumableConnection::dispose))
                            /*Resumption is impossible: no need to return control to ResumableConnection*/
                            .then(Mono.never())));
    return this;
  }

  @Override
  public void reconnect(ResumeAwareConnection connection) {
    resumableConnection.reconnect(connection);
  }

  @Override
  public DuplexConnection resumableConnection() {
    return resumableConnection;
  }

  @Override
  public ResumeToken token() {
    return resumeToken;
  }

  public KeepAliveData keepAliveData() {
    return keepAliveData;
  }

  private Mono<Void> sendFrame(ByteBuf frame) {
    logger.info("Sending Resume frame: {}", frame);
    return resumableConnection.sendOne(frame).onErrorResume(e -> Mono.empty());
  }

  private static ResumptionState stateFromFrame(ByteBuf resumeFrame) {
    long peerPos = ResumeFrameFlyweight.firstAvailableClientPos(resumeFrame);
    long peerImpliedPos = ResumeFrameFlyweight.lastReceivedServerPos(resumeFrame);
    resumeFrame.release();
    return ResumptionState.fromClient(peerPos, peerImpliedPos);
  }

  private static RejectedResumeException errorFrameThrowable(Throwable err) {
    String msg;
    if (err instanceof ResumeStateException) {
      ResumeStateException resumeException = ((ResumeStateException) err);
      msg =
          String.format(
              "resumption_pos=[ remote: %s, local: %s]",
              resumeException.remoteState(), resumeException.localState());
    } else {
      msg = String.format("resume_internal_error: %s", err.getMessage());
    }
    return new RejectedResumeException(msg);
  }
}
