/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.examples.transport.tcp.lease.simple;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseReceiver;
import io.rsocket.lease.LeaseSender;
import io.rsocket.lease.Leases;
import io.rsocket.lease.MissingLeaseException;
import io.rsocket.lease.RequestTracker;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

public class LeaseExample {

  private static final Logger logger = LoggerFactory.getLogger(LeaseExample.class);

  private static final String SERVER_TAG = "server";
  private static final String CLIENT_TAG = "client";

  public static void main(String[] args) {
    // Queue for incoming messages represented as Flux
    // Imagine that every fireAndForget that is pushed is processed by a worker

    int queueCapacity = 50;
    BlockingQueue<String> messagesQueue = new ArrayBlockingQueue<>(queueCapacity);

    // emulating a worker that process data from the queue
    Thread workerThread =
        new Thread(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  String message = messagesQueue.take();
                  logger.info("Process message {}", message);
                  Thread.sleep(500); // emulating processing
                }
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    workerThread.start();

    CloseableChannel server =
        RSocketServer.create(
                (setup, sendingSocket) ->
                    Mono.just(
                        new RSocket() {
                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            // add element. if overflows errors and terminates execution
                            // specifically to show that lease can limit rate of fnf requests in
                            // that example
                            try {
                              if (!messagesQueue.offer(payload.getDataUtf8())) {
                                logger.error("Queue has been overflowed. Terminating execution");
                                sendingSocket.dispose();
                                workerThread.interrupt();
                              }
                            } finally {
                              payload.release();
                            }
                            return Mono.empty();
                          }
                        }))
            .lease(() -> Leases.create().sender(new LeaseCalculator(SERVER_TAG, messagesQueue)))
            .bindNow(TcpServerTransport.create("localhost", 7000));

    SimpleLeaseReceiver receiver = new SimpleLeaseReceiver(CLIENT_TAG);
    RSocket clientRSocket =
        RSocketConnector.create()
            .lease(() -> Leases.create().receiver(receiver))
            .connect(TcpClientTransport.create(server.address()))
            .block();

    Objects.requireNonNull(clientRSocket);

    // generate stream of fnfs
    Flux.generate(
            () -> 0L,
            (state, sink) -> {
              sink.next(state);
              return state + 1;
            })
        // here we wait for the first lease for the responder side and start execution
        // on if there is allowance
        .delaySubscription(receiver.notifyWhenNewLease().then())
        .concatMap(
            tick -> {
              logger.info("Requesting FireAndForget({})", tick);
              return Mono.defer(() -> clientRSocket.fireAndForget(ByteBufPayload.create("" + tick)))
                  .retryWhen(
                      Retry.indefinitely()
                          // ensures that error is the result of missed lease
                          .filter(t -> t instanceof MissingLeaseException)
                          .doBeforeRetryAsync(
                              rs -> {
                                // here we create a mechanism to delay the retry until
                                // the new lease allowance comes in.
                                logger.info("Ran out of leases {}", rs);
                                return receiver.notifyWhenNewLease().then();
                              }));
            })
        .blockLast();

    clientRSocket.onClose().block();
    server.dispose();
  }

  /**
   * This is a class responsible for making decision on whether Responder is ready to receive new
   * FireAndForget or not base in the number of messages enqueued. <br>
   * In the nutshell this is responder-side rate-limiter logic which is created for every new
   * connection.<br>
   * In real-world projects this class has to issue leases based on real metrics
   */
  private static class LeaseCalculator implements LeaseSender<RequestTracker> {
    final String tag;
    final BlockingQueue<?> queue;

    public LeaseCalculator(String tag, BlockingQueue<?> queue) {
      this.tag = tag;
      this.queue = queue;
    }

    @Override
    public Flux<Lease> send(@Nullable RequestTracker leaseStats) {
      Duration ttlDuration = Duration.ofSeconds(5);
      // The interval function is used only for the demo purpose and should not be
      // considered as the way to issue leases.
      // For advanced RateLimiting with Leasing
      // consider adopting https://github.com/Netflix/concurrency-limits#server-limiter
      return Flux.interval(Duration.ZERO, ttlDuration.dividedBy(2))
          .handle(
              (__, sink) -> {
                // put queue.remainingCapacity() + 1 here if you want to observe that app is
                // terminated  because of the queue overflowing
                int requests = queue.remainingCapacity();

                // reissue new lease only if queue has remaining capacity to
                // accept more requests
                if (requests > 0) {
                  long ttl = ttlDuration.toMillis();
                  sink.next(Lease.create((int) ttl, requests));
                }
              });
    }
  }

  private static class SimpleLeaseReceiver implements LeaseReceiver {
    final String tag;
    final ReplayProcessor<Lease> lastLeaseReplay = ReplayProcessor.cacheLast();

    public SimpleLeaseReceiver(String tag) {
      this.tag = tag;
    }

    @Override
    public void receive(Flux<Lease> receivedLeases) {
      receivedLeases.subscribe(
          l -> {
            logger.info(
                "{} received leases - ttl: {}, requests: {}",
                tag,
                l.getTimeToLiveMillis(),
                l.getAllowedRequests());
            lastLeaseReplay.onNext(l);
          });
    }

    /**
     * This method allows to listen to new incoming leases and delay some action (e.g . retry) until
     * new valid lease has come in
     */
    public Mono<Lease> notifyWhenNewLease() {
      return lastLeaseReplay.filter(l -> l.isValid()).next();
    }
  }
}
