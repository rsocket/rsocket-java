/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.aeron.internal.reactivestreams;

import java.net.SocketAddress;

/** SocketAddress that represents an Aeron Channel */
public class AeronSocketAddress extends SocketAddress {
  private static final String FORMAT = "%s?endpoint=%s:%d";
  private static final long serialVersionUID = -7691068719112973697L;
  private final String protocol;
  private final String host;
  private final int port;
  private final String channel;

  private AeronSocketAddress(String protocol, String host, int port) {
    this.protocol = protocol;
    this.host = host;
    this.port = port;
    this.channel = String.format(FORMAT, protocol, host, port);
  }

  public static AeronSocketAddress create(String protocol, String host, int port) {
    return new AeronSocketAddress(protocol, host, port);
  }

  public String getProtocol() {
    return protocol;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getChannel() {
    return channel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AeronSocketAddress that = (AeronSocketAddress) o;

    return channel != null ? channel.equals(that.channel) : that.channel == null;
  }

  @Override
  public int hashCode() {
    return channel != null ? channel.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "AeronSocketAddress{"
        + "protocol='"
        + protocol
        + '\''
        + ", host='"
        + host
        + '\''
        + ", port="
        + port
        + ", channel='"
        + channel
        + '\''
        + '}';
  }
}
