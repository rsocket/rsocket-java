/*
 * Copyright 2015-2020 the original author or authors.
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
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ResumeOkFrameCodec;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class ClientRSocketSession implements RSocketSession<Mono<DuplexConnection>> {
  private static final Logger logger = LoggerFactory.getLogger(ClientRSocketSession.class);

  //  private final ResumableDuplexConnection resumableConnection;
  private volatile Mono<DuplexConnection> newConnection;
  private volatile ByteBuf resumeToken;
  private final ByteBufAllocator allocator;

  public ClientRSocketSession(
      DuplexConnection duplexConnection,
      Duration resumeSessionDuration,
      Retry retry,
      ResumableFramesStore resumableFramesStore,
      Duration resumeStreamTimeout,
      boolean cleanupStoreOnKeepAlive) {
    this.allocator = duplexConnection.alloc();
    //    this.resumableConnection =
    //        new ResumableDuplexConnection(
    //            "client",
    //            duplexConnection,
    //            resumableFramesStore,
    //            resumeStreamTimeout,
    //            cleanupStoreOnKeepAlive);

    /*session completed: release token initially retained in resumeToken(ByteBuf)*/
    onClose().doFinally(s -> resumeToken.release()).subscribe();
    //
    //    resumableConnection
    //        .connectionErrors()
    //        .flatMap(
    //            err -> {
    //              logger.debug("Client session connection error. Starting new connection");
    //              AtomicBoolean once = new AtomicBoolean();
    //              return newConnection
    //                  .delaySubscription(
    //                      once.compareAndSet(false, true)
    //                          ? retry.generateCompanion(Flux.just(new RetrySignal(err)))
    //                          : Mono.empty())
    //                  .retryWhen(retry)
    //                  .timeout(resumeSessionDuration);
    //            })
    //        .map(ClientServerInputMultiplexer::new)
    //        .subscribe(
    //            multiplexer -> {
    //              /*reconnect resumable connection*/
    //              reconnect(multiplexer.asClientServerConnection());
    //              long impliedPosition = resumableConnection.impliedPosition();
    //              long position = resumableConnection.position();
    //              logger.debug(
    //                  "Client ResumableConnection reconnected. Sending RESUME frame with state:
    // [impliedPos: {}, pos: {}]",
    //                  impliedPosition,
    //                  position);
    //              /*Connection is established again: send RESUME frame to server, listen for
    // RESUME_OK*/
    //              sendFrame(
    //                      ResumeFrameCodec.encode(
    //                          allocator,
    //                          /*retain so token is not released once sent as part of resume
    // frame*/
    //                          resumeToken.retain(),
    //                          impliedPosition,
    //                          position))
    //                  .then(multiplexer.asSetupConnection().receive().next())
    //                  .subscribe(this::resumeWith);
    //            },
    //            err -> {
    //              logger.debug("Client ResumableConnection reconnect timeout");
    //              resumableConnection.dispose();
    //            });
  }

  @Override
  public ClientRSocketSession continueWith(Mono<DuplexConnection> connectionFactory) {
    this.newConnection = connectionFactory;
    return this;
  }

  @Override
  public ClientRSocketSession resumeWith(ByteBuf resumeOkFrame) {
    logger.debug("ResumeOK FRAME received");
    long remotePos = remotePos(resumeOkFrame);
    long remoteImpliedPos = remoteImpliedPos(resumeOkFrame);
    resumeOkFrame.release();

    //    resumableConnection.resume(
    //        remotePos,
    //        remoteImpliedPos,
    //        pos ->
    //            pos.then()
    //                /*Resumption is impossible: send CONNECTION_ERROR*/
    //                .onErrorResume(
    //                    err ->
    //                        sendFrame(
    //                                ErrorFrameCodec.encode(
    //                                    allocator, 0, errorFrameThrowable(remoteImpliedPos)))
    //                            .then(Mono.fromRunnable(resumableConnection::dispose))
    //                            /*Resumption is impossible: no need to return control to
    // ResumableConnection*/
    //                            .then(Mono.never())));
    return this;
  }

  public ClientRSocketSession resumeToken(ByteBuf resumeToken) {
    /*retain so token is not released once sent as part of setup frame*/
    this.resumeToken = resumeToken.retain();
    return this;
  }

  @Override
  public void reconnect(DuplexConnection connection) {
    //    resumableConnection.reconnect(connection);
  }
  //
  //  @Override
  //  public ResumableDuplexConnection resumableConnection() {
  //    return resumableConnection;
  //  }

  @Override
  public ByteBuf token() {
    return resumeToken;
  }

  private Mono<Void> sendFrame(ByteBuf frame) {
    //    return resumableConnection.sendOne(frame).onErrorResume(err -> Mono.empty());
    return null;
  }

  private static long remoteImpliedPos(ByteBuf resumeOkFrame) {
    return ResumeOkFrameCodec.lastReceivedClientPos(resumeOkFrame);
  }

  private static long remotePos(ByteBuf resumeOkFrame) {
    return -1;
  }

  private static ConnectionErrorException errorFrameThrowable(long impliedPos) {
    return new ConnectionErrorException("resumption_server_pos=[" + impliedPos + "]");
  }

  private static class RetrySignal implements Retry.RetrySignal {

    private final Throwable ex;

    RetrySignal(Throwable ex) {
      this.ex = ex;
    }

    @Override
    public long totalRetries() {
      return 0;
    }

    @Override
    public long totalRetriesInARow() {
      return 0;
    }

    @Override
    public Throwable failure() {
      return ex;
    }
  }
}
