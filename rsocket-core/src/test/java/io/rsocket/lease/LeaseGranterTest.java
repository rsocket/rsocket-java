/*
package io.rsocket.lease;

import io.netty.buffer.Unpooled;
import io.rsocket.Frame;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

abstract class LeaseGranterTest {
  protected Flux<Lease> timeLimitedLeases(LeaseManager leaseManager, Duration duration) {
    return leaseManager.getLeases().publishOn(Schedulers.elastic()).takeUntilOther(delay(duration));
  }

  protected Frame newLeaseFrame(int numberOfRequests, int ttl) {
    return Frame.Lease.from(ttl, numberOfRequests, Unpooled.EMPTY_BUFFER);
  }

  protected Mono<Long> delay(Duration duration) {
    return Mono.delay(duration);
  }
}
*/
