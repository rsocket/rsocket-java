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

package io.rsocket.lease;

import static org.junit.Assert.*;

import io.netty.buffer.Unpooled;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class LeaseImplTest {

  @Test
  public void emptyLeaseNoAvailability() {
    LeaseImpl empty = LeaseImpl.empty();
    Assertions.assertTrue(empty.isEmpty());
    Assertions.assertFalse(empty.isValid());
    Assertions.assertEquals(0.0, empty.availability(), 1e-5);
  }

  @Test
  public void emptyLeaseUseNoAvailability() {
    LeaseImpl empty = LeaseImpl.empty();
    boolean success = empty.use();
    assertFalse(success);
    Assertions.assertEquals(0.0, empty.availability(), 1e-5);
  }

  @Test
  public void leaseAvailability() {
    LeaseImpl lease = LeaseImpl.create(2, 100, Unpooled.EMPTY_BUFFER);
    Assertions.assertEquals(1.0, lease.availability(), 1e-5);
  }

  @Test
  public void leaseUseDecreasesAvailability() {
    LeaseImpl lease = LeaseImpl.create(30_000, 2, Unpooled.EMPTY_BUFFER);
    boolean success = lease.use();
    Assertions.assertTrue(success);
    Assertions.assertEquals(0.5, lease.availability(), 1e-5);
    Assertions.assertTrue(lease.isValid());
    success = lease.use();
    Assertions.assertTrue(success);
    Assertions.assertEquals(0.0, lease.availability(), 1e-5);
    Assertions.assertFalse(lease.isValid());
    Assertions.assertEquals(0, lease.getAllowedRequests());
    success = lease.use();
    Assertions.assertFalse(success);
  }

  @Test
  public void leaseTimeout() {
    int numberOfRequests = 1;
    LeaseImpl lease = LeaseImpl.create(1, numberOfRequests, Unpooled.EMPTY_BUFFER);
    Mono.delay(Duration.ofMillis(100)).block();
    boolean success = lease.use();
    Assertions.assertFalse(success);
    Assertions.assertTrue(lease.isExpired());
    Assertions.assertEquals(numberOfRequests, lease.getAllowedRequests());
    Assertions.assertFalse(lease.isValid());
  }

  @Test
  public void useLeaseChangesAllowedRequests() {
    int numberOfRequests = 2;
    LeaseImpl lease = LeaseImpl.create(30_000, numberOfRequests, Unpooled.EMPTY_BUFFER);
    lease.use();
    assertEquals(numberOfRequests - 1, lease.getAllowedRequests());
  }
}
