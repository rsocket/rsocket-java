/*
 * Copyright 2015-2023 the original author or authors.
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

package io.rsocket.transport.local;

import io.rsocket.test.TransportTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

final class LocalResumableTransportTest implements TransportTest {

  private TransportPair transportPair;

  @BeforeEach
  void createTestPair(TestInfo testInfo) {
    transportPair =
        new TransportPair<>(
            () ->
                "LocalResumableTransportTest-"
                    + testInfo.getDisplayName()
                    + "-"
                    + UUID.randomUUID(),
            (address, server, allocator) -> LocalClientTransport.create(address, allocator),
            (address, allocator) -> LocalServerTransport.create(address),
            false,
            true);
  }

  @Override
  public Duration getTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public TransportPair getTransportPair() {
    return transportPair;
  }
}
