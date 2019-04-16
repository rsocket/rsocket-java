package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;

public interface KeepAliveFramesAcceptor {

  void receive(ByteBuf keepAliveFrame);

  void dispose();
}
