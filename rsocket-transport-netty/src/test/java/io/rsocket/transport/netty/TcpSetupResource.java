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

package io.rsocket.transport.netty;

import io.rsocket.test.extension.AbstractExtension;
import io.rsocket.test.extension.SetupResource;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;

public class TcpSetupResource extends SetupResource<InetSocketAddress, NettyContextCloseable> {
  private TcpSetupResource() {
    super(
        () -> InetSocketAddress.createUnresolved("localhost", 0),
        (address, server) -> TcpClientTransport.create(server.address()),
        TcpServerTransport::create);
  }

  public static class Extension extends AbstractExtension {
    @Override
    protected Class<?> type() {
      return TcpSetupResource.class;
    }
  }
}
