/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.loadbalance;

import io.rsocket.transport.ClientTransport;

/**
 * Simple container for a key and a {@link ClientTransport}, representing a specific target for
 * loadbalancing purposes. The key is used to compare previous and new targets when refreshing the
 * list of target to use. The transport is used to connect to the target.
 *
 * @since 1.1
 */
public class LoadbalanceTarget {

  final String key;
  final ClientTransport transport;

  private LoadbalanceTarget(String key, ClientTransport transport) {
    this.key = key;
    this.transport = transport;
  }

  /** Return the key for this target. */
  public String getKey() {
    return key;
  }

  /** Return the transport to use to connect to the target. */
  public ClientTransport getTransport() {
    return transport;
  }

  /**
   * Create a an instance of {@link LoadbalanceTarget} with the given key and {@link
   * ClientTransport}. The key can be anything that can be used to identify identical targets, e.g.
   * a SocketAddress, URL, etc.
   *
   * @param key the key to use to identify identical targets
   * @param transport the transport to use for connecting to the target
   * @return the created instance
   */
  public static LoadbalanceTarget from(String key, ClientTransport transport) {
    return new LoadbalanceTarget(key, transport);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    LoadbalanceTarget that = (LoadbalanceTarget) other;
    return key.equals(that.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }
}
