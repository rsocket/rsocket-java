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

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.rsocket.test.TransportTest;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

final class AeronTransportTest extends TransportTest<InetSocketAddress, AeronServer> {

  static final MediaDriver mediaDriver =
      MediaDriver.launch(
          new MediaDriver.Context().threadingMode(ThreadingMode.DEDICATED).dirDeleteOnStart(true));

  static final Aeron clientAeron = Aeron.connect();
  static final Aeron serverAeron = Aeron.connect();
  static final EventLoopGroup eventLoopGroup = EventLoopGroup.create(4);

  @Override
  protected TransportPair<InetSocketAddress, AeronServer> createTransportPair() {
    return new AeronTransportPair(mediaDriver, clientAeron, serverAeron);
  }

  static class AeronTransportPair extends TransportPair<InetSocketAddress, AeronServer> {

    final MediaDriver mediaDriver;
    final Aeron clientAeron;
    final Aeron serverAeron;

    public AeronTransportPair(MediaDriver driver, Aeron clientAeron, Aeron serverAeron) {
      super(
          () ->
              InetSocketAddress.createUnresolved(
                  "0.0.0.0", ThreadLocalRandom.current().nextInt(20000) + 5000),
          (address, server, allocator) ->
              AeronClientTransport.createIpc(clientAeron, eventLoopGroup),
          (address, allocator) -> AeronServerTransport.createIpc(serverAeron, eventLoopGroup),
          false,
          false,
          false,
          Duration.ofMinutes(2));
      this.mediaDriver = driver;
      this.clientAeron = clientAeron;
      this.serverAeron = serverAeron;
    }

    @Override
    public void dispose() {
      super.dispose();
      //      CloseHelper.quietCloseAll(clientAeron, serverAeron);
    }
  }
}
