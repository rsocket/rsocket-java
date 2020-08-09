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
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.frame.ResumeFrameCodec;
import java.time.Duration;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class ServerRSocketSession implements RSocketSession<DuplexConnection> {
  private static final Logger logger = LoggerFactory.getLogger(ServerRSocketSession.class);

  //  private final ResumableDuplexConnection resumableConnection;
  /*used instead of EmitterProcessor because its autocancel=false capability had no expected effect*/
  private final FluxProcessor<DuplexConnection, DuplexConnection> newConnections =
      ReplayProcessor.create(0);
  private final ByteBufAllocator allocator;
  private final ByteBuf resumeToken;

  public ServerRSocketSession(
      DuplexConnection duplexConnection,
      Duration resumeSessionDuration,
      Duration resumeStreamTimeout,
      Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory,
      ByteBuf resumeToken,
      boolean cleanupStoreOnKeepAlive) {
    this.allocator = duplexConnection.alloc();
    this.resumeToken = resumeToken;
    //    this.resumableConnection =
    //        new ResumableDuplexConnection(
    //            "server",
    //            duplexConnection,
    //            resumeStoreFactory.apply(resumeToken),
    //            resumeStreamTimeout,
    //            cleanupStoreOnKeepAlive);
    //
    //    Mono<DuplexConnection> timeout =
    //        resumableConnection
    //            .connectionErrors()
    //            .flatMap(
    //                err -> {
    //                  logger.debug("Starting session timeout due to error", err);
    //                  return newConnections
    //                      .next()
    //                      .doOnNext(c -> logger.debug("Connection after error: {}", c))
    //                      .timeout(resumeSessionDuration);
    //                })
    //            .then()
    //            .cast(DuplexConnection.class);

    newConnections
        //        .mergeWith(timeout)
        .subscribe(
        connection -> {
          reconnect(connection);
          logger.debug("Server ResumableConnection reconnected: {}", connection);
        },
        err -> {
          logger.debug("Server ResumableConnection reconnect timeout");
          //              resumableConnection.dispose();
        });
  }

  @Override
  public ServerRSocketSession continueWith(DuplexConnection connectionFactory) {
    logger.debug("Server continued with connection: {}", connectionFactory);
    newConnections.onNext(connectionFactory);
    return this;
  }

  @Override
  public ServerRSocketSession resumeWith(ByteBuf resumeFrame) {
    logger.debug("Resume FRAME received");
    long remotePos = remotePos(resumeFrame);
    long remoteImpliedPos = remoteImpliedPos(resumeFrame);
    resumeFrame.release();

    //    resumableConnection.resume(
    //        remotePos,
    //        remoteImpliedPos,
    //        pos ->
    //            pos.flatMap(impliedPos -> sendFrame(ResumeOkFrameCodec.encode(allocator,
    // impliedPos)))
    //                .onErrorResume(
    //                    err ->
    //                        sendFrame(ErrorFrameCodec.encode(allocator, 0,
    // errorFrameThrowable(err)))
    //                            .then(Mono.fromRunnable(resumableConnection::dispose))
    //                            /*Resumption is impossible: no need to return control to
    // ResumableConnection*/
    //                            .then(Mono.never())));
    return this;
  }

  @Override
  public void reconnect(DuplexConnection connection) {
    //    resumableConnection.reconnect(connection);
  }

  //  @Override
  //  public ResumableDuplexConnection resumableConnection() {
  //    return resumableConnection;
  //  }
  //
  @Override
  public ByteBuf token() {
    return resumeToken;
  }

  private Mono<Void> sendFrame(ByteBuf frame) {
    logger.debug("Sending Resume frame: {}", frame);
    //    return resumableConnection.sendOne(frame).onErrorResume(e -> Mono.empty());
    return null;
  }

  private static long remotePos(ByteBuf resumeFrame) {
    return ResumeFrameCodec.firstAvailableClientPos(resumeFrame);
  }

  private static long remoteImpliedPos(ByteBuf resumeFrame) {
    return ResumeFrameCodec.lastReceivedServerPos(resumeFrame);
  }

  private static RejectedResumeException errorFrameThrowable(Throwable err) {
    String msg;
    if (err instanceof ResumeStateException) {
      ResumeStateException resumeException = ((ResumeStateException) err);
      msg =
          String.format(
              "resumption_pos=[ remote: { pos: %d, impliedPos: %d }, local: { pos: %d, impliedPos: %d }]",
              resumeException.getRemotePos(),
              resumeException.getRemoteImpliedPos(),
              resumeException.getLocalPos(),
              resumeException.getLocalImpliedPos());
    } else {
      msg = String.format("resume_internal_error: %s", err.getMessage());
    }
    return new RejectedResumeException(msg);
  }
}
