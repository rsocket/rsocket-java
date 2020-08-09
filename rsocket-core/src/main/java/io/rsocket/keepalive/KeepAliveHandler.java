package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.keepalive.KeepAliveSupport.KeepAlive;
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
    //    private final ResumableDuplexConnection resumableDuplexConnection;

    public ResumableKeepAliveHandler(DuplexConnection resumableDuplexConnection) {
      //      this.resumableDuplexConnection = resumableDuplexConnection;
    }

    @Override
    public KeepAliveFramesAcceptor start(
        KeepAliveSupport keepAliveSupport,
        Consumer<ByteBuf> onSendKeepAliveFrame,
        Consumer<KeepAlive> onTimeout) {
      //      resumableDuplexConnection.onResume(keepAliveSupport::start);
      //      resumableDuplexConnection.onDisconnect(keepAliveSupport::stop);
      return keepAliveSupport
          //          .resumeState(resumableDuplexConnection)
          .onSendKeepAliveFrame(onSendKeepAliveFrame)
          //          .onTimeout(keepAlive -> resumableDuplexConnection.disconnect())
          .start();
    }
  }
}
