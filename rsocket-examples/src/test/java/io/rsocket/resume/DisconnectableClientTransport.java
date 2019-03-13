/*
 * Copyright 2015-2019 the original author or authors.
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

package io.rsocket.resume;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;

class DisconnectableClientTransport implements ClientTransport {
  private final ClientTransport clientTransport;
  private final AtomicReference<DuplexConnection> curConnection = new AtomicReference<>();
  private long nextConnectPermitMillis;

  public DisconnectableClientTransport(ClientTransport clientTransport) {
    this.clientTransport = clientTransport;
  }

  @Override
  public Mono<DuplexConnection> connect(int mtu) {
    return Mono.defer(
        () ->
            now() < nextConnectPermitMillis
                ? Mono.error(new ClosedChannelException())
                : clientTransport
                    .connect(mtu)
                    .map(
                        c -> {
                          if (curConnection.compareAndSet(null, c)) {
                            return c;
                          } else {
                            throw new IllegalStateException(
                                "Transport supports at most 1 connection");
                          }
                        }));
  }

  public void disconnect() {
    disconnectFor(Duration.ZERO);
  }

  public void disconnectPermanently() {
    disconnectFor(Duration.ofDays(42));
  }

  public void disconnectFor(Duration cooldown) {
    DuplexConnection cur = curConnection.getAndSet(null);
    if (cur != null) {
      nextConnectPermitMillis = now() + cooldown.toMillis();
      cur.dispose();
    } else {
      throw new IllegalStateException("Trying to disconnect while not connected");
    }
  }

  private static long now() {
    return System.currentTimeMillis();
  }
}
