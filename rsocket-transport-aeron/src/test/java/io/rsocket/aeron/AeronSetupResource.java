/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.aeron;

import io.rsocket.Closeable;
import io.rsocket.aeron.client.AeronClientTransport;
import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.Constants;
import io.rsocket.aeron.internal.DefaultAeronWrapper;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.SingleThreadedEventLoop;
import io.rsocket.aeron.internal.reactivestreams.AeronClientChannelConnector;
import io.rsocket.aeron.internal.reactivestreams.AeronSocketAddress;
import io.rsocket.aeron.server.AeronServerTransport;
import io.rsocket.test.extension.AbstractExtension;
import io.rsocket.test.extension.SetupResource;

class AeronSetupResource extends SetupResource<AeronSocketAddress, Closeable> {
  private static final AeronSocketAddress ADDRESS =
      AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

  static {
    MediaDriverHolder.getInstance();
    AeronWrapper aeronWrapper = new DefaultAeronWrapper();

    EventLoop serverEventLoop = new SingleThreadedEventLoop("server");
    server = new AeronServerTransport(aeronWrapper, ADDRESS, serverEventLoop);

    // Create Client Connector
    EventLoop clientEventLoop = new SingleThreadedEventLoop("client");

    AeronClientChannelConnector.AeronClientConfig config =
        AeronClientChannelConnector.AeronClientConfig.create(
            ADDRESS,
            ADDRESS,
            Constants.CLIENT_STREAM_ID,
            Constants.SERVER_STREAM_ID,
            clientEventLoop);

    AeronClientChannelConnector connector =
        AeronClientChannelConnector.create(aeronWrapper, ADDRESS, clientEventLoop);

    client = new AeronClientTransport(connector, config);
  }

  private static final AeronServerTransport server;
  private static final AeronClientTransport client;

  private AeronSetupResource() {
    super(() -> ADDRESS, (address, server) -> client, address -> server);
  }

  public static class Extension extends AbstractExtension {
    @Override
    protected Class<?> type() {
      return AeronSetupResource.class;
    }
  }
}
