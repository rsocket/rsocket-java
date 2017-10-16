/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.aeron;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import io.rsocket.Payload;
import io.rsocket.util.PayloadImpl;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;

@Disabled
@ExtendWith(AeronSetupResource.Extension.class)
public class ClientServerTest {
  @Test
  public void testFireNForget10(AeronSetupResource setup) {
    long outputCount = assertTimeout(ofSeconds(10), () ->
        Flux.range(1, 10)
            .flatMap(i -> setup.getRSocket().fireAndForget(new PayloadImpl("hello", "metadata")))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block());

    assertEquals(0, outputCount);
  }

  @Test
  public void testPushMetadata10(AeronSetupResource setup) {
    long outputCount = assertTimeout(ofSeconds(10), () ->
        Flux.range(1, 10)
            .flatMap(i -> setup.getRSocket().metadataPush(new PayloadImpl("", "metadata")))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block());

    assertEquals(0, outputCount);
  }

  @Test
  public void testRequestResponse1(AeronSetupResource setup) {
    long outputCount = assertTimeout(ofSeconds(5000), () ->
        Flux.range(1, 1)
            .flatMap(
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block());

    assertEquals(1, outputCount);
  }

  @Test
  public void testRequestResponse10(AeronSetupResource setup) {
    long outputCount = assertTimeout(ofSeconds(2), () ->
        Flux.range(1, 10)
            .flatMap(
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block());

    assertEquals(10, outputCount);
  }

  @Test
  public void testRequestResponse100(AeronSetupResource setup) {
    long outputCount = assertTimeout(ofSeconds(2), () ->
        Flux.range(1, 100)
            .flatMap(
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block());

    assertEquals(100, outputCount);
  }

  @Test
  public void testRequestResponse10_000(AeronSetupResource setup) {
    long outputCount = assertTimeout(ofSeconds(5), () ->
        Flux.range(1, 10_000)
            .flatMap(
                i ->
                    setup
                        .getRSocket()
                        .requestResponse(new PayloadImpl("hello", "metadata"))
                        .map(Payload::getDataUtf8))
            .doOnError(Throwable::printStackTrace)
            .count()
            .block());

    assertEquals(10_000, outputCount);
  }

  @Test
  public void testRequestStream(AeronSetupResource setup) {
    long count = assertTimeout(ofSeconds(10), () -> {
      Flux<Payload> publisher =
          setup.getRSocket().requestStream(new PayloadImpl("hello", "metadata"));

      return publisher.take(5).count().block();
    });

    assertEquals(5, count);
  }
}
