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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.concurrent.Queues;

public class InMemoryResumableFramesStore implements ResumableFramesStore {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryResumableFramesStore.class);
  private static final long SAVE_REQUEST_SIZE = Long.MAX_VALUE;

  private final MonoProcessor<Void> disposed = MonoProcessor.create();
  private volatile long position;
  private volatile long impliedPosition;
  private volatile int cacheSize;
  private final Queue<ByteBuf> cachedFrames;
  private final String tag;
  private final int cacheLimit;
  private volatile int upstreamFrameRefCnt;

  public InMemoryResumableFramesStore(String tag, int cacheSizeBytes) {
    this.tag = tag;
    this.cacheLimit = cacheSizeBytes;
    this.cachedFrames = cachedFramesQueue(cacheSizeBytes);
  }

  public Mono<Void> saveFrames(Flux<ByteBuf> frames) {
    MonoProcessor<Void> completed = MonoProcessor.create();
    frames
        .doFinally(s -> completed.onComplete())
        .subscribe(new FramesSubscriber(SAVE_REQUEST_SIZE));
    return completed;
  }

  @Override
  public void releaseFrames(long remoteImpliedPos) {
    long pos = position;
    logger.debug(
        "{} Removing frames for local: {}, remote implied: {}", tag, pos, remoteImpliedPos);
    long removeSize = Math.max(0, remoteImpliedPos - pos);
    while (removeSize > 0) {
      ByteBuf cachedFrame = cachedFrames.poll();
      if (cachedFrame != null) {
        removeSize -= releaseTailFrame(cachedFrame);
      } else {
        break;
      }
    }
    if (removeSize > 0) {
      throw new IllegalStateException(
          String.format(
              "Local and remote state disagreement: "
                  + "need to remove additional %d bytes, but cache is empty",
              removeSize));
    } else if (removeSize < 0) {
      throw new IllegalStateException(
          "Local and remote state disagreement: " + "local and remote frame sizes are not equal");
    } else {
      logger.debug("{} Removed frames. Current cache size: {}", tag, cacheSize);
    }
  }

  @Override
  public Flux<ByteBuf> resumeStream() {
    return Flux.create(
        s -> {
          int size = cachedFrames.size();
          int refCnt = upstreamFrameRefCnt;
          logger.debug("{} Resuming stream size: {}", tag, size);
          /*spsc queue has no iterator - iterating by consuming*/
          for (int i = 0; i < size; i++) {
            ByteBuf frame = cachedFrames.poll();
            /*in the event of connection termination some frames
             * are not released on DuplexConnection*/
            if (frame.refCnt() == refCnt) {
              frame.retain();
            }
            cachedFrames.offer(frame);
            s.next(frame);
          }
          s.complete();
          logger.debug("{} Resuming stream completed", tag);
        });
  }

  @Override
  public long framePosition() {
    return position;
  }

  @Override
  public long frameImpliedPosition() {
    return impliedPosition;
  }

  @Override
  public void resumableFrameReceived(ByteBuf frame) {
    /*called on transport thread so non-atomic on volatile is safe*/
    impliedPosition += frame.readableBytes();
  }

  @Override
  public Mono<Void> onClose() {
    return disposed;
  }

  @Override
  public void dispose() {
    cacheSize = 0;
    ByteBuf frame = cachedFrames.poll();
    while (frame != null) {
      frame.release();
      frame = cachedFrames.poll();
    }
    disposed.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return disposed.isTerminated();
  }

  /* this method and saveFrame() won't be called concurrently,
   * so non-atomic on volatile is safe*/
  private int releaseTailFrame(ByteBuf content) {
    int frameSize = content.readableBytes();
    cacheSize -= frameSize;
    position += frameSize;
    content.release();
    return frameSize;
  }

  /*this method and releaseTailFrame() won't be called concurrently,
   * so non-atomic on volatile is safe*/
  private void saveFrame(ByteBuf frame) {
    if (upstreamFrameRefCnt == 0) {
      upstreamFrameRefCnt = frame.refCnt();
    }

    int frameSize = frame.readableBytes();
    long availableSize = cacheLimit - cacheSize;
    while (availableSize < frameSize) {
      ByteBuf cachedFrame = cachedFrames.poll();
      if (cachedFrame != null) {
        availableSize += releaseTailFrame(cachedFrame);
      } else {
        break;
      }
    }
    if (availableSize >= frameSize) {
      cachedFrames.offer(frame.retain());
      cacheSize += frameSize;
    }
  }

  private static Queue<ByteBuf> cachedFramesQueue(int size) {
    return Queues.<ByteBuf>get(size).get();
  }

  private class FramesSubscriber implements Subscriber<ByteBuf> {
    private final long firstRequestSize;
    private final long refillSize;
    private int received;
    private Subscription s;

    public FramesSubscriber(long requestSize) {
      this.firstRequestSize = requestSize;
      this.refillSize = firstRequestSize / 2;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      s.request(firstRequestSize);
    }

    @Override
    public void onNext(ByteBuf byteBuf) {
      saveFrame(byteBuf);
      if (firstRequestSize != Long.MAX_VALUE && ++received == refillSize) {
        received = 0;
        s.request(refillSize);
      }
    }

    @Override
    public void onError(Throwable t) {
      logger.info("unexpected onError signal: {}, {}", t.getClass(), t.getMessage());
    }

    @Override
    public void onComplete() {}
  }
}
