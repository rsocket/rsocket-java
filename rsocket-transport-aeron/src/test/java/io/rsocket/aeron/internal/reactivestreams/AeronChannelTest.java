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
package io.rsocket.aeron.internal.reactivestreams;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.rsocket.aeron.MediaDriverHolder;
import io.rsocket.aeron.internal.Constants;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.SingleThreadedEventLoop;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.agrona.BitUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;

/** */
@Ignore("travis does not like me")
public class AeronChannelTest {
  static {
    // System.setProperty("aeron.publication.linger.timeout", String.valueOf(50_000_000_000L));
    // System.setProperty("aeron.client.liveness.timeout", String.valueOf(50_000_000_000L));
    MediaDriverHolder.getInstance();
  }

  @Test
  @Ignore
  public void testPing() {

    int count = 5_000_000;
    CountDownLatch countDownLatch = new CountDownLatch(count);

    CountDownLatch sync = new CountDownLatch(2);
    Aeron.Context ctx = new Aeron.Context();

    // ctx.publicationConnectionTimeout(TimeUnit.MINUTES.toNanos(5));

    ctx.availableImageHandler(
        image -> {
          System.out.println(
              "name image subscription => "
                  + image.subscription().channel()
                  + " streamId => "
                  + image.subscription().streamId()
                  + " registrationId => "
                  + image.subscription().registrationId());
          sync.countDown();
        });

    ctx.unavailableImageHandler(
        image ->
            System.out.println(
                "=== unavailable image name image subscription => "
                    + image.subscription().channel()
                    + " streamId => "
                    + image.subscription().streamId()
                    + " registrationId => "
                    + image.subscription().registrationId()));
    /*ctx.errorHandler(t -> {
       /* StringWriter writer = new StringWriter();
        PrintWriter w = new PrintWriter(writer);
        t.printStackTrace(w);

        w.flush();*

       // System.out.println("\nGOT AERON ERROR => \n [" + writer.toString() + "]\n\n");
    });*/

    ctx.driverTimeoutMs(Integer.MAX_VALUE);
    Aeron aeron = Aeron.connect(ctx);
    /*
            Subscription serverSubscription = aeron.addSubscription("aeron:ipc", Constants.SERVER_STREAM_ID);
            Publication serverPublication = aeron.addPublication("aeron:ipc", Constants.CLIENT_STREAM_ID);

            Subscription clientSubscription = aeron.addSubscription("aeron:ipc", Constants.CLIENT_STREAM_ID);
            Publication clientPublication = aeron.addPublication("aeron:ipc", Constants.SERVER_STREAM_ID);
    */

    Subscription serverSubscription =
        aeron.addSubscription("aeron:udp?endpoint=localhost:39791", Constants.SERVER_STREAM_ID);
    System.out.println(
        "serverSubscription registration id => " + serverSubscription.registrationId());

    Publication serverPublication =
        aeron.addPublication("aeron:udp?endpoint=localhost:39790", Constants.CLIENT_STREAM_ID);

    Subscription clientSubscription =
        aeron.addSubscription("aeron:udp?endpoint=localhost:39790", Constants.CLIENT_STREAM_ID);

    System.out.println(
        "clientSubscription registration id => " + clientSubscription.registrationId());
    Publication clientPublication =
        aeron.addPublication("aeron:udp?endpoint=localhost:39791", Constants.SERVER_STREAM_ID);

    try {
      sync.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    EventLoop serverLoop = new SingleThreadedEventLoop("server");

    AeronOutPublisher publisher =
        new AeronOutPublisher(
            "server", clientPublication.sessionId(), serverSubscription, serverLoop);
    publisher
        .doOnNext(i -> countDownLatch.countDown())
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    AeronInSubscriber aeronInSubscriber = new AeronInSubscriber("client", clientPublication);

    Flux<UnsafeBuffer> unsafeBufferObservable =
        Flux.range(1, count)
            // .doOnNext(i -> LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50)))
            // .doOnNext(i -> System.out.println(Thread.currentThread() + " => client sending => " +
            // i))
            .map(
                i -> {
                  UnsafeBuffer buffer = new UnsafeBuffer(new byte[BitUtil.SIZE_OF_INT]);
                  buffer.putInt(0, i);
                  return buffer;
                })
            // .doOnRequest(l -> System.out.println("Client reuqested => " + l))
            .doOnError(Throwable::printStackTrace)
            .doOnComplete(() -> System.out.println("Im done"));

    unsafeBufferObservable.subscribe(aeronInSubscriber);

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      LangUtil.rethrowUnchecked(e);
    }
    System.out.println("HERE!!!!");
  }

  @Test(timeout = 2_000)
  public void testPingPong_10() {
    pingPong(10);
  }

  @Test(timeout = 2_000)
  public void testPingPong_100() {
    pingPong(100);
  }

  @Test(timeout = 5_000)
  public void testPingPong_300() {
    pingPong(300);
  }

  @Test(timeout = 5_000)
  public void testPingPong_1_000() {
    pingPong(1_000);
  }

  @Test(timeout = 15_000)
  public void testPingPong_10_000() {
    pingPong(10_000);
  }

  @Ignore
  @Test(timeout = 5_000)
  public void testPingPong_100_000() {
    pingPong(100_000);
  }

  @Ignore
  @Test(timeout = 15_000)
  public void testPingPong_1_000_000() {
    pingPong(1_000_000);
  }

  @Test(timeout = 50_000)
  @Ignore
  public void testPingPong_10_000_000() {
    pingPong(10_000_000);
  }

  @Test
  @Ignore
  public void testPingPongAlot() {
    pingPong(100_000_000);
  }

  private void pingPong(int count) {

    CountDownLatch sync = new CountDownLatch(2);
    Aeron.Context ctx = new Aeron.Context();
    ctx.availableImageHandler(
        image -> {
          System.out.println(
              "name image subscription => "
                  + image.subscription().channel()
                  + " streamId => "
                  + image.subscription().streamId()
                  + " registrationId => "
                  + image.subscription().registrationId());
          sync.countDown();
        });

    ctx.unavailableImageHandler(
        image ->
            System.out.println(
                "=== unavailable image name image subscription => "
                    + image.subscription().channel()
                    + " streamId => "
                    + image.subscription().streamId()
                    + " registrationId => "
                    + image.subscription().registrationId()));

    /*ctx.errorHandler(t -> {
       /* StringWriter writer = new StringWriter();
        PrintWriter w = new PrintWriter(writer);
        t.printStackTrace(w);

        w.flush();*

       // System.out.println("\nGOT AERON ERROR => \n [" + writer.toString() + "]\n\n");
    });*/

    // ctx.driverTimeoutMs(Integer.MAX_VALUE);
    Aeron aeron = Aeron.connect(ctx);

    Subscription serverSubscription =
        aeron.addSubscription("aeron:ipc", Constants.SERVER_STREAM_ID);
    Publication serverPublication = aeron.addPublication("aeron:ipc", Constants.CLIENT_STREAM_ID);

    Subscription clientSubscription =
        aeron.addSubscription("aeron:ipc", Constants.CLIENT_STREAM_ID);
    Publication clientPublication = aeron.addPublication("aeron:ipc", Constants.SERVER_STREAM_ID);

    /*
            Subscription serverSubscription = aeron.addSubscription("udp://localhost:39791", Constants.SERVER_STREAM_ID);
            System.out.println("serverSubscription registration id => " + serverSubscription.registrationId());

            Publication serverPublication = aeron.addPublication("udp://localhost:39790", Constants.CLIENT_STREAM_ID);

            Subscription clientSubscription = aeron.addSubscription("udp://localhost:39790", Constants.CLIENT_STREAM_ID);

            System.out.println("clientSubscription registration id => " + clientSubscription.registrationId());
            Publication clientPublication = aeron.addPublication("udp://localhost:39791", Constants.SERVER_STREAM_ID);
    */
    try {
      sync.await();
    } catch (InterruptedException e) {
      LangUtil.rethrowUnchecked(e);
    }

    SingleThreadedEventLoop serverLoop = new SingleThreadedEventLoop("server");
    SingleThreadedEventLoop clientLoop = new SingleThreadedEventLoop("client");

    AeronChannel serverChannel =
        new AeronChannel(
            "server",
            serverPublication,
            serverSubscription,
            serverLoop,
            clientPublication.sessionId());

    System.out.println("created server channel");

    CountDownLatch latch = new CountDownLatch(count);

    serverChannel
        .receive()
        // latch.countDown();
        // System.out.println("received -> " + f.getInt(0));
        .flatMap(serverChannel::send, 32)
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    AeronChannel clientChannel =
        new AeronChannel(
            "client",
            clientPublication,
            clientSubscription,
            clientLoop,
            serverPublication.sessionId());

    clientChannel
        .receive()
        .doOnNext(
            l -> {
              synchronized (latch) {
                latch.countDown();
                if (latch.getCount() % 10_000 == 0) {
                  System.out.println("mod of client got back -> " + latch.getCount());
                }
                //  if (latch.getCount() < 10_000) {
                //      System.out.println("client got back -> " + latch.getCount());
                // }
              }
            })
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    byte[] bytes = new byte[8];
    ThreadLocalRandom.current().nextBytes(bytes);

    Flux.range(1, count)
        // .doOnRequest(l -> System.out.println("requested => " + l))
        .flatMap(
            i -> {
              // System.out.println("Sending -> " + i);

              // UnsafeBuffer b = new UnsafeBuffer(new byte[BitUtil.SIZE_OF_INT]);
              UnsafeBuffer b = new UnsafeBuffer(bytes);
              b.putInt(0, i);

              return clientChannel.send(b);
            },
            8)
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    try {
      latch.await();
    } catch (Exception t) {
      LangUtil.rethrowUnchecked(t);
    }
  }
}
