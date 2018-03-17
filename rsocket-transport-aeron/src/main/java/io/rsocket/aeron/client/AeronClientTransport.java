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

package io.rsocket.aeron.client;

import io.rsocket.DuplexConnection;
import io.rsocket.aeron.AeronDuplexConnection;
import io.rsocket.aeron.internal.reactivestreams.AeronChannel;
import io.rsocket.aeron.internal.reactivestreams.AeronClientChannelConnector;
import io.rsocket.transport.ClientTransport;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** {@link ClientTransport} implementation that uses Aeron as a transport */
public class AeronClientTransport implements ClientTransport {
  private final AeronClientChannelConnector connector;
  private final AeronClientChannelConnector.AeronClientConfig config;

  public AeronClientTransport(
      AeronClientChannelConnector connector, AeronClientChannelConnector.AeronClientConfig config) {
    Objects.requireNonNull(config);
    Objects.requireNonNull(connector);
    this.connector = connector;
    this.config = config;
  }

  @Override
  public Mono<DuplexConnection> connect() {
    Publisher<AeronChannel> channelPublisher = connector.apply(config);

    return Mono.from(channelPublisher)
        .map(aeronChannel -> new AeronDuplexConnection("client", aeronChannel));
  }
}
