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
import java.nio.charset.StandardCharsets;
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
            .flatMap(i -> setup.getRSocket().fireAndForget(new PayloadImpl("hello", "metadata")))
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

  @Test(timeout = 10000)
  public void testRequestResponse1() {
    long outputCount =
        Flux.range(1, 1)
            .flatMap(
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
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
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(10, outputCount);
  }

  @Test(timeout = 10000)
  public void testRequestResponse100() {
    long outputCount =
        Flux.range(1, 100)
            .flatMap(
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
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
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block();

    assertEquals(10_000, outputCount);
  }

  @Test(timeout = 10000)
  public void testRequestStream() {
    Flux<Payload> publisher =
        setup.getRSocket().requestStream(new PayloadImpl("hello", "metadata"));

    long count = publisher.take(5).count().block();

    assertEquals(5, count);
  }

  @Test(timeout = 10000)
  public void testRequestStreamWithRequestN() {
    CountdownBaseSubscriber ts = new CountdownBaseSubscriber();
    ts.expect(5);

    setup.getRSocket().requestStream(new PayloadImpl("hello", "metadata")).subscribe(ts);

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

    setup.getRSocket().requestStream(new PayloadImpl("hello", "metadata")).subscribe(ts);

    ts.expect(5);

    ts.await();
    assertEquals(5, ts.count());

    ts.expect(5);
    ts.await();
    ts.cancel();

    assertEquals(10, ts.count());
  }
}
