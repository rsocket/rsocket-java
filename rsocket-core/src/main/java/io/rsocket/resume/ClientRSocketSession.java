package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.ResumeOkFrameFlyweight;
import io.rsocket.internal.ClientServerInputMultiplexer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ClientRSocketSession implements RSocketSession<Mono<? extends ResumeAwareConnection>> {
  private static final Logger logger = LoggerFactory.getLogger(ClientRSocketSession.class);

  private final ResumableDuplexConnection resumableConnection;
  private volatile Mono<? extends ResumeAwareConnection> newConnection;
  private volatile ResumeToken resumeToken;
  private ByteBufAllocator allocator;

  public ClientRSocketSession(
      ByteBufAllocator allocator,
      ResumeAwareConnection duplexConnection,
      ClientResumeConfiguration config) {
    this.allocator = Objects.requireNonNull(allocator);
    this.resumableConnection =
        new ResumableDuplexConnection(
            "client", duplexConnection, ResumedFramesCalculator.ofClient, config.cacheSizeFrames());

    resumableConnection
        .connectionErrors()
        .flatMap(
            err -> {
              logger.info("Client session connection error. Starting new connection");
              ResumeStrategy reconnectOnError = config.resumptionStrategy().get();
              ClientResume clientResume = new ClientResume(config.sessionDuration(), resumeToken);
              AtomicBoolean once = new AtomicBoolean();
              return newConnection
                  .delaySubscription(
                      once.compareAndSet(false, true)
                          ? reconnectOnError.apply(clientResume, err)
                          : Mono.empty())
                  .retryWhen(
                      errors ->
                          errors
                              .doOnNext(
                                  retryErr ->
                                      logger.info("Resumption reconnection error: {}", retryErr))
                              .flatMap(
                                  retryErr ->
                                      Mono.from(reconnectOnError.apply(clientResume, retryErr))
                                          .doOnNext(v -> logger.info("Retrying with: {}", v))))
                  .timeout(config.sessionDuration());
            })
        .map(ClientServerInputMultiplexer::new)
        .subscribe(
            multiplexer -> {
              /*reconnect resumable connection*/
              reconnect(multiplexer.asClientServerConnection());

              ResumptionState state = resumableConnection.state();
              logger.info(
                  "Client ResumableConnection reconnected. Sending RESUME frame with state: {}",
                  state);
              /*Connection is established again: send RESUME frame to server, listen for RESUME_OK*/
              sendFrame(
                  ResumeFrameFlyweight.encode(
                      allocator,
                      resumeToken.toByteArray(), state.impliedPosition(), state.position()))
                  .then(multiplexer.asSetupConnection().receive().next())
                  .subscribe(this::resumeWith);
            },
            err -> {
              logger.info("Client ResumableConnection reconnect timeout");
              resumableConnection.dispose();
            });
  }

  @Override
  public ClientRSocketSession continueWith(Mono<? extends ResumeAwareConnection> newConnection) {
    this.newConnection = newConnection;
    return this;
  }

  @Override
  public ClientRSocketSession resumeWith(ByteBuf resumeOkFrame) {
    logger.info("ResumeOK FRAME received");
    ResumptionState resumptionState = stateFromFrame(resumeOkFrame);
    resumableConnection.resume(
        resumptionState,
        pos ->
            pos.then()
                /*Resumption is impossible: send CONNECTION_ERROR*/
                .onErrorResume(
                    err ->
                        sendFrame(
                            ErrorFrameFlyweight.encode(
                                allocator,
                                0,
                                errorFrameThrowable(resumptionState.impliedPosition())))
                            .then(Mono.fromRunnable(resumableConnection::dispose))
                            /*Resumption is impossible: no need to return control to ResumableConnection*/
                            .then(Mono.never())));
    return this;
  }

  public ClientRSocketSession resumeWith(ResumeToken resumeToken) {
    this.resumeToken = resumeToken;
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

  private Mono<Void> sendFrame(ByteBuf frame) {
    return resumableConnection.sendOne(frame).onErrorResume(err -> Mono.empty());
  }

  private static ResumptionState stateFromFrame(ByteBuf resumeOkFrame) {
    long impliedPos = ResumeOkFrameFlyweight.lastReceivedClientPos(resumeOkFrame);
    resumeOkFrame.release();
    return ResumptionState.fromServer(impliedPos);
  }

  private static ConnectionErrorException errorFrameThrowable(long impliedPos) {
    return new ConnectionErrorException("resumption_server_pos=[" + impliedPos + "]");
  }
}
