package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import reactor.core.Disposable;

public interface KeepAliveFramesAcceptor extends Disposable {

  void receive(ByteBuf keepAliveFrame);
}
