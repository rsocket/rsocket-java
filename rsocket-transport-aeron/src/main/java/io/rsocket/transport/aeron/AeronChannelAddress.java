package io.rsocket.transport.aeron;

import io.netty.util.internal.ObjectUtil;
import java.net.SocketAddress;

public final class AeronChannelAddress extends SocketAddress {
  private static final long serialVersionUID = -6934618000832236893L;

  final String channel;

  public AeronChannelAddress(String channel) {
    this.channel = ObjectUtil.checkNotNull(channel, "channel");
  }

  /** The Aeron Subscription Channel. */
  public String channel() {
    return channel;
  }

  @Override
  public String toString() {
    return channel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AeronChannelAddress)) {
      return false;
    }

    return ((AeronChannelAddress) o).channel.equals(channel);
  }

  @Override
  public int hashCode() {
    return channel.hashCode();
  }
}
