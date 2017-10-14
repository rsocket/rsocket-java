/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.test;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.test.extension.SetupResource;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public abstract class BaseClientServerTest<T extends SetupResource<?, ? extends Closeable>> {
  private static final Duration TIMEOUT = ofSeconds(10);

  @Test
  public void testFireNForget10(T setup) {
    long outputCount = assertTimeout(
        TIMEOUT,
        () ->
            Flux.range(1, 10)
                .flatMap(i -> setup.getRSocket().fireAndForget(testPayload(i)))
                .doOnError(Throwable::printStackTrace)
                .count()
                .block());

    assertEquals(0, outputCount);
  }

  @Test
  public void testPushMetadata10(T setup) {
    long outputCount = assertTimeout(
        TIMEOUT,
        () ->
            Flux.range(1, 10)
                .flatMap(i -> setup.getRSocket().metadataPush(new PayloadImpl("", "metadata")))
                .doOnError(Throwable::printStackTrace)
                .count()
                .block());

    assertEquals(0, outputCount);
  }

  @Test
  public void testRequestResponse1(T setup) {
    long outputCount = assertTimeout(
        TIMEOUT,
        () ->
            Flux.range(1, 1)
                .flatMap(
                    i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
                .doOnError(Throwable::printStackTrace)
                .count()
                .block());

    assertEquals(1, outputCount);
  }

  @Test
  public void testRequestResponse10(T setup) {
    long outputCount = assertTimeout(
        TIMEOUT,
        () ->
            Flux.range(1, 10)
                .flatMap(
                    i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
                .doOnError(Throwable::printStackTrace)
                .count()
                .block());

    assertEquals(10, outputCount);
  }

  private Payload testPayload(int metadataPresent) {
    String metadata;
    switch (metadataPresent % 5) {
      case 0:
        metadata = null;
        break;
      case 1:
        metadata = "";
        break;
      default:
        metadata = "metadata";
        break;
    }
    return new PayloadImpl("hello", metadata);
  }

  @Test
  public void testRequestResponse100(T setup) {
    long outputCount = assertTimeout(
        TIMEOUT,
        () ->
            Flux.range(1, 100)
                .flatMap(
                    i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
                .doOnError(Throwable::printStackTrace)
                .count()
                .block());

    assertEquals(100, outputCount);
  }

  @Test
  public void testRequestResponse10_000(T setup) {
    long outputCount = assertTimeout(
        TIMEOUT,
        () ->
            Flux.range(1, 10_000)
                .flatMap(
                    i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
                .doOnError(Throwable::printStackTrace)
                .count()
                .block());

    assertEquals(10_000, outputCount);
  }

  @Test
  public void testRequestStream(T setup) {
    long count = assertTimeout(
        TIMEOUT,
        () -> {
          Flux<Payload> publisher = setup.getRSocket().requestStream(testPayload(3));

          return publisher.take(5).count().block();
        });
    assertEquals(5, count);
  }

  @Test
  public void testRequestStreamWithRequestN(T setup) {
    CountdownBaseSubscriber subscriber= assertTimeout(
        TIMEOUT,
        () -> {
          CountdownBaseSubscriber ts = new CountdownBaseSubscriber();
          ts.expect(5);

          setup.getRSocket().requestStream(testPayload(3)).subscribe(ts);

          ts.await();
          assertEquals(5, ts.count());

          ts.expect(5);
          ts.await();
          ts.cancel();
          return ts;
        });

    assertEquals(10, subscriber.count());
  }

  @Test
  public void testRequestStreamWithDelayedRequestN(T setup) {
    CountdownBaseSubscriber subscriber= assertTimeout(
        TIMEOUT,
        () -> {
          CountdownBaseSubscriber ts = new CountdownBaseSubscriber();

          setup.getRSocket().requestStream(testPayload(3)).subscribe(ts);

          ts.expect(5);

          ts.await();
          assertEquals(5, ts.count());

          ts.expect(5);
          ts.await();
          ts.cancel();
          return ts;
        });
    assertEquals(10, subscriber.count());
  }

  @Disabled
  @Test
  public void testChannel0(T setup) {
    long count = assertTimeout(
        TIMEOUT,
        () -> {
          Flux<Payload> publisher = setup.getRSocket().requestChannel(Flux.empty());

          return publisher.count().block();
        });

    assertEquals(0, count);
  }

  @Test
  public void testChannel1(T setup) {
    long count = assertTimeout(
        TIMEOUT,
        () -> {
          Flux<Payload> publisher = setup.getRSocket().requestChannel(Flux.just(testPayload(0)));

          return publisher.count().block();
        });

    assertEquals(1, count);
  }

  @Test
  public void testChannel3(T setup) {
    long count = assertTimeout(
        TIMEOUT,
        () -> {
          Flux<Payload> publisher =
              setup
                  .getRSocket()
                  .requestChannel(Flux.just(testPayload(0), testPayload(1), testPayload(2)));

          return publisher.count().block();
        });

    assertEquals(3, count);
  }
}
