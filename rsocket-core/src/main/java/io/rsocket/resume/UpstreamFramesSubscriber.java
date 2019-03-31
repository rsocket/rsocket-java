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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Operators;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

class UpstreamFramesSubscriber implements Subscriber<ByteBuf>, Disposable {
  private static final Logger logger = LoggerFactory.getLogger(UpstreamFramesSubscriber.class);

  private final FluxProcessor<Object, Object> actions = UnicastProcessor.create().serialize();
  private final AtomicBoolean disposed = new AtomicBoolean();
  private final Consumer<ByteBuf> itemConsumer;
  private final Disposable downstreamRequestDisposable;
  private final Disposable resumeSaveStreamDisposable;

  private volatile Subscription subs;
  private volatile boolean resumeStarted;
  private final Queue<ByteBuf> framesCache;
  private long request;
  private long downStreamRequestN;
  private long resumeSaveStreamRequestN;

  UpstreamFramesSubscriber(
      int estimatedDownstreamRequest,
      Flux<Long> downstreamRequests,
      Flux<Long> resumeSaveStreamRequests,
      Consumer<ByteBuf> itemConsumer) {
    this.itemConsumer = itemConsumer;
    this.framesCache = Queues.<ByteBuf>unbounded(estimatedDownstreamRequest).get();

    downstreamRequestDisposable = downstreamRequests.subscribe(requestN -> requestN(0, requestN));

    resumeSaveStreamDisposable =
        resumeSaveStreamRequests.subscribe(requestN -> requestN(requestN, 0));

    dispatch(actions);
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.subs = s;
    if (!isDisposed()) {
      doRequest();
    } else {
      s.cancel();
    }
  }

  @Override
  public void onNext(ByteBuf item) {
    actions.onNext(item);
  }

  @Override
  public void onError(Throwable t) {
    dispose();
  }

  @Override
  public void onComplete() {
    dispose();
  }

  public void resumeStart() {
    actions.onNext(new ResumeStart());
  }

  public void resumeComplete() {
    actions.onNext(new ResumeComplete());
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      releaseCache();
      if (subs != null) {
        subs.cancel();
      }
      resumeSaveStreamDisposable.dispose();
      downstreamRequestDisposable.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return disposed.get();
  }

  private void dispatch(Flux<?> p) {
    p.subscribe(
        o -> {
          if (o instanceof ByteBuf) {
            processFrame(((ByteBuf) o));
          } else {
            ((Runnable) o).run();
          }
        });
  }

  private void requestN(long resumeStreamRequest, long downStreamRequest) {
    synchronized (this) {
      downStreamRequestN = Operators.addCap(downStreamRequestN, downStreamRequest);
      resumeSaveStreamRequestN = Operators.addCap(resumeSaveStreamRequestN, resumeStreamRequest);

      long requests = Math.min(downStreamRequestN, resumeSaveStreamRequestN);
      if (requests > 0) {
        downStreamRequestN -= requests;
        resumeSaveStreamRequestN -= requests;
        logger.debug("Upstream subscriber requestN: {}", requests);
        request = Operators.addCap(request, requests);
      }
    }
    doRequest();
  }

  private void doRequest() {
    if (subs != null && !resumeStarted) {
      synchronized (this) {
        long r = request;
        if (r > 0) {
          subs.request(r);
          request = 0;
        }
      }
    }
  }

  private void releaseCache() {
    ByteBuf frame = framesCache.poll();
    while (frame != null && frame.refCnt() > 0) {
      frame.release();
    }
  }

  private void doResumeStart() {
    resumeStarted = true;
  }

  private void doResumeComplete() {
    ByteBuf frame = framesCache.poll();
    while (frame != null) {
      itemConsumer.accept(frame);
      frame = framesCache.poll();
    }
    resumeStarted = false;
    doRequest();
  }

  private void processFrame(ByteBuf item) {
    if (resumeStarted) {
      framesCache.offer(item);
    } else {
      itemConsumer.accept(item);
    }
  }

  private class ResumeStart implements Runnable {

    @Override
    public void run() {
      doResumeStart();
    }
  }

  private class ResumeComplete implements Runnable {

    @Override
    public void run() {
      doResumeComplete();
    }
  }
}
