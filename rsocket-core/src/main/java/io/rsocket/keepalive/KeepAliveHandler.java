package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.keepalive.KeepAliveSupport.KeepAlive;
import io.rsocket.resume.RSocketSession;
import io.rsocket.resume.ResumableDuplexConnection;
import io.rsocket.resume.ResumeStateHolder;
import java.util.function.Consumer;

public interface KeepAliveHandler {

  KeepAliveFramesAcceptor start(
      KeepAliveSupport keepAliveSupport,
      Consumer<ByteBuf> onFrameSent,
      Consumer<KeepAlive> onTimeout);

  class DefaultKeepAliveHandler implements KeepAliveHandler {
    private final Closeable duplexConnection;

    public DefaultKeepAliveHandler(Closeable duplexConnection) {
      this.duplexConnection = duplexConnection;
    }

    @Override
    public KeepAliveFramesAcceptor start(
        KeepAliveSupport keepAliveSupport,
        Consumer<ByteBuf> onSendKeepAliveFrame,
        Consumer<KeepAlive> onTimeout) {
      duplexConnection.onClose().doFinally(s -> keepAliveSupport.stop()).subscribe();
      return keepAliveSupport
          .onSendKeepAliveFrame(onSendKeepAliveFrame)
          .onTimeout(onTimeout)
          .start();
    }
  }

  class ResumableKeepAliveHandler implements KeepAliveHandler {

    private final ResumableDuplexConnection resumableDuplexConnection;
    private final RSocketSession rSocketSession;
    private final ResumeStateHolder resumeStateHolder;

    public ResumableKeepAliveHandler(
        ResumableDuplexConnection resumableDuplexConnection,
        RSocketSession rSocketSession,
        ResumeStateHolder resumeStateHolder) {
      this.resumableDuplexConnection = resumableDuplexConnection;
      this.rSocketSession = rSocketSession;
      this.resumeStateHolder = resumeStateHolder;
    }

    @Override
    public KeepAliveFramesAcceptor start(
        KeepAliveSupport keepAliveSupport,
        Consumer<ByteBuf> onSendKeepAliveFrame,
        Consumer<KeepAlive> onTimeout) {

      rSocketSession.setKeepAliveSupport(keepAliveSupport);

      return keepAliveSupport
          .resumeState(resumeStateHolder)
          .onSendKeepAliveFrame(onSendKeepAliveFrame)
          .onTimeout(keepAlive -> resumableDuplexConnection.disconnect())
          .start();
    }
  }
}
