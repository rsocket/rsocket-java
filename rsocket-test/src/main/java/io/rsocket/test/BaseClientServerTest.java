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

import static org.junit.Assert.assertEquals;

import io.rsocket.Payload;
import io.rsocket.util.PayloadImpl;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import reactor.core.publisher.Flux;

public abstract class BaseClientServerTest<T extends ClientSetupRule<?, ?>> {
  @Rule public final T setup = createClientServer();

  protected abstract T createClientServer();

  @Test(timeout = 10000)
  public void testFireNForget10() {
    long outputCount =
        Flux.range(1, 10)
            .flatMap(i -> setup.getRSocket().fireAndForget(testPayload(i)))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(0, outputCount);
  }

  @Test(timeout = 10000)
  public void testPushMetadata10() {
    long outputCount =
        Flux.range(1, 10)
            .flatMap(i -> setup.getRSocket().metadataPush(new PayloadImpl("", "metadata")))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(0, outputCount);
  }

  @Test // (timeout = 10000)
  public void testRequestResponse1() {
    long outputCount =
        Flux.range(1, 1)
            .flatMap(
                i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(1, outputCount);
  }

  @Test(timeout = 10000)
  public void testRequestResponse10() {
    long outputCount =
        Flux.range(1, 10)
            .flatMap(
                i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

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

  @Test(timeout = 10000)
  public void testRequestResponse100() {
    long outputCount =
        Flux.range(1, 100)
            .flatMap(
                i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(100, outputCount);
  }

  @Test(timeout = 20000)
  public void testRequestResponse10_000() {
    long outputCount =
        Flux.range(1, 10_000)
            .flatMap(
                i -> setup.getRSocket().requestResponse(testPayload(i)).map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(10_000, outputCount);
  }

  @Test(timeout = 10000)
  public void testRequestStream() {
    Flux<Payload> publisher = setup.getRSocket().requestStream(testPayload(3));

    long count = publisher.take(5).count().block();

    assertEquals(5, count);
  }

  @Test(timeout = 10000)
  public void testRequestStreamAll() {
    Flux<Payload> publisher = setup.getRSocket().requestStream(testPayload(3));

    long count = publisher.count().block();

    assertEquals(10000, count);
  }

  @Test(timeout = 10000)
  public void testRequestStreamWithRequestN() {
    CountdownBaseSubscriber ts = new CountdownBaseSubscriber();
    ts.expect(5);

    setup.getRSocket().requestStream(testPayload(3)).subscribe(ts);

    ts.await();
    assertEquals(5, ts.count());

    ts.expect(5);
    ts.await();
    ts.cancel();

    assertEquals(10, ts.count());
  }

  @Test(timeout = 10000)
  public void testRequestStreamWithDelayedRequestN() {
    CountdownBaseSubscriber ts = new CountdownBaseSubscriber();

    setup.getRSocket().requestStream(testPayload(3)).subscribe(ts);

    ts.expect(5);

    ts.await();
    assertEquals(5, ts.count());

    ts.expect(5);
    ts.await();
    ts.cancel();

    assertEquals(10, ts.count());
  }

  @Test(timeout = 10000)
  public void testChannel0() {
    Flux<Payload> publisher = setup.getRSocket().requestChannel(Flux.empty());

    long count = publisher.count().block();

    assertEquals(0, count);
  }

  @Test(timeout = 10000)
  public void testChannel1() {
    Flux<Payload> publisher = setup.getRSocket().requestChannel(Flux.just(testPayload(0)));

    long count = publisher.count().block();

    assertEquals(1, count);
  }

  @Test(timeout = 10000)
  public void testChannel3() {
    Flux<Payload> publisher =
        setup
            .getRSocket()
            .requestChannel(Flux.just(testPayload(0), testPayload(1), testPayload(2)));

    long count = publisher.count().block();

    assertEquals(3, count);
  }

  @Test(timeout = 10000)
  public void testChannel512() {
    Flux<Payload> payloads = Flux.range(1, 512).map(i -> new PayloadImpl("hello " + i));

    long count = setup.getRSocket().requestChannel(payloads).count().block();

    assertEquals(512, count);
  }

  @Test(timeout = 30000)
  public void testChannel20_000() {
    Flux<Payload> payloads = Flux.range(1, 20_000).map(i -> new PayloadImpl("hello " + i));

    long count = setup.getRSocket().requestChannel(payloads).count().block();

    assertEquals(20_000, count);
  }

  @Test(timeout = 60_000)
  public void testChannel200_000() {
    Flux<Payload> payloads = Flux.range(1, 200_000).map(i -> new PayloadImpl("hello " + i));

    long count = setup.getRSocket().requestChannel(payloads).count().block();

    assertEquals(200_000, count);
  }

  @Test(timeout = 60_000)
  @Ignore
  public void testChannel2_000_000() {
    AtomicInteger counter = new AtomicInteger(0);

    Flux<Payload> payloads = Flux.range(1, 2_000_000).map(i -> new PayloadImpl("hello " + i));
    long count = setup.getRSocket().requestChannel(payloads).count().block();

    assertEquals(2_000_000, count);
  }
}
