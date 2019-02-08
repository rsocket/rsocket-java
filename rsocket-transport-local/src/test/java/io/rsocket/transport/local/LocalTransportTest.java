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

package io.rsocket.transport.local;

import io.rsocket.test.TransportTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

final class LocalTransportTest {// implements TransportTest {
/*
TODO // think this has a memory leak or something in the local connection now that needs to be checked into. the test
TODO // isn't very happy when run from commandline i the command line
  private static final AtomicInteger UNIQUE_NAME_GENERATOR = new AtomicInteger();

  private final TransportPair transportPair =
      new TransportPair<>(
          () -> "test" + UNIQUE_NAME_GENERATOR.incrementAndGet(),
          (address, server) -> LocalClientTransport.create(address),
          LocalServerTransport::create);
  
  @Override
  @Test
  public void requestChannel512() {
  
  }
  
  @Override
  public Duration getTimeout() {
    return Duration.ofSeconds(10);
  }

  @Override
  public TransportPair getTransportPair() {
    return transportPair;
  }*/
}
