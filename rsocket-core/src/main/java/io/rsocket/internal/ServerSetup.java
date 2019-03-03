package io.rsocket.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.resume.*;
import io.rsocket.util.ConnectionUtils;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

public interface ServerSetup {
  /*accept connection as SETUP*/
  Mono<Void> acceptRSocketSetup(
      ByteBuf frame,
      ClientServerInputMultiplexer multiplexer,
      Function<ClientServerInputMultiplexer, Mono<Void>> then);

  /*accept connection as RESUME*/
  Mono<Void> acceptRSocketResume(ByteBuf frame, ClientServerInputMultiplexer multiplexer);

  /*get KEEP-ALIVE timings based on start frame: SETUP (directly) /RESUME (lookup by resume token)*/
  Function<ByteBuf, Mono<KeepAliveData>> keepAliveData();

  default void dispose() {}

  class DefaultServerSetup implements ServerSetup {
    private final ByteBufAllocator allocator;

    public DefaultServerSetup(ByteBufAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public Mono<Void> acceptRSocketSetup(
        ByteBuf frame,
        ClientServerInputMultiplexer multiplexer,
        Function<ClientServerInputMultiplexer, Mono<Void>> then) {

      if (!ResumeToken.fromBytes(SetupFrameFlyweight.resumeToken(frame)).isEmpty()) {
        return sendError(
                multiplexer,
                new UnsupportedSetupException("resume not supported"))
                .doFinally(signalType -> {
                  frame.release();
                  multiplexer.dispose();
                });
      } else {
        return then.apply(multiplexer);
      }
    }

    @Override
    public Mono<Void> acceptRSocketResume(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {

      return sendError(
              multiplexer,
              new RejectedResumeException("resume not supported"))
              .doFinally(signalType -> {
                frame.release();
                multiplexer.dispose();
              });
    }

    @Override
    public Function<ByteBuf, Mono<KeepAliveData>> keepAliveData() {
      return frame -> {
        if (FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP) {
          return Mono.just(
              new KeepAliveData(
                  SetupFrameFlyweight.keepAliveInterval(frame),
                  SetupFrameFlyweight.keepAliveMaxLifetime(frame)));
        } else {
          return Mono.never();
        }
      };
    }

    private Mono<Void> sendError(
            ClientServerInputMultiplexer multiplexer,
            Exception exception) {
      return ConnectionUtils.sendError(allocator, multiplexer, exception);
    }
  }

  class ResumableServerSetup implements ServerSetup {
    private final ByteBufAllocator allocator;
    private final SessionManager sessionManager;
    private final ServerResumeConfiguration resumeConfig;

    public ResumableServerSetup(
        ByteBufAllocator allocator,
        SessionManager sessionManager,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Function<? super ResumeToken, ? extends ResumeStore> resumeStoreFactory) {
      this.allocator = allocator;
      this.sessionManager = sessionManager;
      this.resumeConfig = new ServerResumeConfiguration(
          resumeSessionDuration,
          resumeStreamTimeout,
          resumeStoreFactory);
    }

    @Override
    public Mono<Void> acceptRSocketSetup(
        ByteBuf frame,
        ClientServerInputMultiplexer multiplexer,
        Function<ClientServerInputMultiplexer, Mono<Void>> then) {

      ResumeToken token = ResumeToken.fromBytes(SetupFrameFlyweight.resumeToken(frame));
      if (!token.isEmpty()) {

        KeepAliveData keepAliveData =
            new KeepAliveData(
                SetupFrameFlyweight.keepAliveInterval(frame),
                SetupFrameFlyweight.keepAliveMaxLifetime(frame));

        DuplexConnection resumableConnection =
            sessionManager
                .save(
                    new ServerRSocketSession(
                        allocator,
                        multiplexer.asClientServerConnection(),
                        resumeConfig,
                        keepAliveData,
                        token))
                .resumableConnection();
        return then.apply(new ClientServerInputMultiplexer(resumableConnection));
      } else {
        return then.apply(multiplexer);
      }
    }

    @Override
    public Mono<Void> acceptRSocketResume(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {
      return sessionManager
          .get(ResumeToken.fromBytes(ResumeFrameFlyweight.token(frame)))
          .map(
              session ->
                  session
                      .continueWith(multiplexer.asClientServerConnection())
                      .resumeWith(frame)
                      .onClose()
                      .then())
          .orElseGet(() ->
              sendError(
                  multiplexer,
                  new RejectedResumeException("unknown resume token"))
                  .doFinally(
                      s -> {
                        frame.release();
                        multiplexer.dispose();
                      })
          );
    }

    @Override
    public Function<ByteBuf, Mono<KeepAliveData>> keepAliveData() {
      return frame -> {
        if (FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP) {
          return Mono.just(
              new KeepAliveData(
                  SetupFrameFlyweight.keepAliveInterval(frame),
                  SetupFrameFlyweight.keepAliveMaxLifetime(frame)));
        } else {
          return sessionManager
              .get(ResumeToken.fromBytes(ResumeFrameFlyweight.token(frame)))
              .map(ServerRSocketSession::keepAliveData)
              .map(Mono::just)
              .orElseGet(Mono::never);
        }
      };
    }

    private Mono<Void> sendError(
        ClientServerInputMultiplexer multiplexer,
        Exception exception) {
      return ConnectionUtils.sendError(allocator, multiplexer, exception);
    }

    @Override
    public void dispose() {
      sessionManager.dispose();
    }
  }
}
