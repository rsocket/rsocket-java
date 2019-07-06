package io.rsocket.transport.netty.server;

import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.rsocket.transport.ServerTransport;

abstract class NettyServerTransport implements ServerTransport<CloseableChannel> {
  final ChannelGroup connectionsGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
}
