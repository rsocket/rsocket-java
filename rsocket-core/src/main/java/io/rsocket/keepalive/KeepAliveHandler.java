package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.rsocket.keepalive.KeepAliveSupport.KeepAlive;
import io.rsocket.resume.ResumableDuplexConnection;
import java.util.function.Consumer;

public interface KeepAliveHandler {

  KeepAliveFramesAcceptor start(
      KeepAliveSupport keepAliveSupport,
      Consumer<ByteBuf> onFrameSent,
      Consumer<KeepAlive> onTimeout);

  class DefaultKeepAliveHandler implements KeepAliveHandler {

    @Override
    public KeepAliveFramesAcceptor start(
        KeepAliveSupport keepAliveSupport,
        Consumer<ByteBuf> onSendKeepAliveFrame,
        Consumer<KeepAlive> onTimeout) {
      return keepAliveSupport
          .onSendKeepAliveFrame(onSendKeepAliveFrame)
          .onTimeout(onTimeout)
          .start();
    }
  }

  class ResumableKeepAliveHandler implements KeepAliveHandler {
    private final ResumableDuplexConnection resumableDuplexConnection;

    public ResumableKeepAliveHandler(ResumableDuplexConnection resumableDuplexConnection) {
      this.resumableDuplexConnection = resumableDuplexConnection;
    }

    @Override
    public KeepAliveFramesAcceptor start(
        KeepAliveSupport keepAliveSupport,
        Consumer<ByteBuf> onSendKeepAliveFrame,
        Consumer<KeepAlive> onTimeout) {
      resumableDuplexConnection.onResumed(keepAliveSupport::start);
      return keepAliveSupport
          .resumeState(resumableDuplexConnection)
          .onSendKeepAliveFrame(onSendKeepAliveFrame)
          .onTimeout(keepAlive -> resumableDuplexConnection.disconnect())
          .start();
    }
  }
}
