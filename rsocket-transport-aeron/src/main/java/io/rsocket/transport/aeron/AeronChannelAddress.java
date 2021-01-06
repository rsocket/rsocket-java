/*
 * Copyright 2015-present the original author or authors.
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
