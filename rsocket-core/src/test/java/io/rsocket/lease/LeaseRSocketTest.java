/*
package io.rsocket.lease;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.NoLeaseException;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.util.PayloadImpl;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LeaseRSocketTest {

  private LeaseManager leaseManager;
  private RSocket leaseRsocket;
  private PayloadImpl payload;

  @Before
  public void setUp() throws Exception {
    leaseManager = new LeaseManager("test");

    RSocketInterceptor enforcer = rSocket -> new LeaseRSocket(rSocket, leaseManager, "test");
    leaseRsocket = enforcer.apply(new EchoRSocket());
    payload = new PayloadImpl("payload");
  }

  @Test(expected = NoLeaseException.class)
  public void missingLease() throws Exception {
    assertNotNull(leaseRsocket.requestResponse(payload).block());
  }

  @Test
  public void availableLease() throws Exception {
    leaseManager.leaseGranted(new LeaseImpl(1, 100_000));
    leaseRsocket.requestResponse(payload).block();
    Lease lease = leaseManager.getLeases().blockFirst();
    assertEquals(0, lease.getAllowedRequests());
  }

  @Test(expected = NoLeaseException.class)
  public void availableLeaseDepleted() throws Exception {
    leaseManager.leaseGranted(new LeaseImpl(1, 100_000));
    leaseRsocket.requestResponse(payload).block();
    leaseRsocket.requestResponse(payload).block();
  }

  static class EchoRSocket implements RSocket {

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return Mono.empty();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.just(payload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Flux.just(payload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return Mono.empty();
    }

    @Override
    public Mono<Void> close() {
      return Mono.empty();
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.empty();
    }
  }
}
*/
