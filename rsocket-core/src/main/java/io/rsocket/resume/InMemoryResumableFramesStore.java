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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.concurrent.Queues;

public class InMemoryResumableFramesStore implements ResumableFramesStore {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryResumableFramesStore.class);

  private final MonoProcessor<Void> disposed = MonoProcessor.create();
  private final AtomicLong position = new AtomicLong();
  private final AtomicLong impliedPosition = new AtomicLong();
  private final AtomicInteger cachedFramesSize = new AtomicInteger();
  private final Queue<ByteBuf> cachedFrames;
  private final String tag;
  private final int cacheLimit;

  public InMemoryResumableFramesStore(String tag, int cacheLimit) {
    this.tag = tag;
    this.cacheLimit = cacheLimit;
    this.cachedFrames = cachedFramesQueue(cacheLimit);
  }

  public Mono<Void> saveFrames(Flux<ByteBuf> frames) {
    return Mono.defer(
        () -> {
          MonoProcessor<Void> completed = MonoProcessor.create();
          frames
              .doFinally(s -> completed.onComplete())
              .subscribe(
                  frame -> {
                    cachedFramesSize.incrementAndGet();
                    cachedFrames.offer(frame.retain());
                    if (cachedFramesSize.get() == cacheLimit) {
                      releaseFrame();
                    }
                  });
          return completed;
        });
  }

  @Override
  public void releaseFrames(long remoteImpliedPos) {
    long pos = position.get();
    logger.debug(
        "{} Removing frames for local: {}, remote implied: {}", tag, pos, remoteImpliedPos);
    long removeCount = Math.max(0, remoteImpliedPos - pos);
    long processedCount = 0;
    while (processedCount < removeCount) {
      releaseFrame();
      processedCount++;
    }
    logger.debug("{} Removed frames. Current size: {}", tag, cachedFramesSize.get());
  }

  private void releaseFrame() {
    ByteBuf content = cachedFrames.poll();
    cachedFramesSize.decrementAndGet();
    content.release();
    position.incrementAndGet();
  }

  @Override
  public Flux<ByteBuf> resumeStream() {
    return Flux.create(
        s -> {
          int size = cachedFramesSize.get();
          logger.debug("{} Resuming stream size: {}", tag, size);
          /*spsc queue has no iterator - iterating by consuming*/
          for (int i = 0; i < size; i++) {
            ByteBuf frame = cachedFrames.poll();
            cachedFrames.offer(frame.retain());
            s.next(frame);
          }
          s.complete();
          logger.debug("{} Resuming stream completed", tag);
        });
  }

  @Override
  public long framePosition() {
    return position.get();
  }

  @Override
  public long frameImpliedPosition() {
    return impliedPosition.get();
  }

  @Override
  public void resumableFrameReceived() {
    impliedPosition.incrementAndGet();
  }

  @Override
  public Mono<Void> onClose() {
    return disposed;
  }

  @Override
  public void dispose() {
    cachedFramesSize.set(0);
    ByteBuf frame = cachedFrames.poll();
    while (frame != null) {
      frame.release();
      frame = cachedFrames.poll();
    }
    disposed.onComplete();
  }

  private static Queue<ByteBuf> cachedFramesQueue(int size) {
    return Queues.<ByteBuf>get(size).get();
  }
}
