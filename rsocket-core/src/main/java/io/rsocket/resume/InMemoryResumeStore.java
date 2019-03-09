package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryResumeStore implements ResumeStore {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryResumeStore.class);

  private final MonoProcessor<Void> disposed = MonoProcessor.create();
  private final AtomicLong position = new AtomicLong();
  private final AtomicLong impliedPosition = new AtomicLong();
  private final AtomicInteger cachedFramesSize = new AtomicInteger();
  private final Queue<ByteBuf> cachedFrames;
  private final String tag;
  private final int cacheLimit;

  public InMemoryResumeStore(String tag, int cacheLimit) {
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
              .subscribe(frame -> {
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
    logger.info("{} Removing frames for local: {}, remote implied: {}", tag, pos, remoteImpliedPos);
    long removeCount = Math.max(0, remoteImpliedPos - pos);
    long processedCount = 0;
    while (processedCount < removeCount) {
      releaseFrame();
      processedCount++;
    }
    logger.info("{} Removed frames. Current size: {}", tag, cachedFramesSize.get());
  }

  private void releaseFrame() {
    ByteBuf content = cachedFrames.poll();
    cachedFramesSize.decrementAndGet();
    content.release();
    position.incrementAndGet();
  }

  @Override
  public Flux<ByteBuf> resumeStream() {
    return Flux.create(s -> {
      int size = cachedFramesSize.get();
      logger.info("{} Resuming stream size: {}", tag, size);
      /*spsc queue has no iterator - iterating by consuming*/
      for (int i = 0; i < size; i++) {
        ByteBuf frame = cachedFrames.poll();
        cachedFrames.offer(frame.retain());
        s.next(frame);
      }
      s.complete();
      logger.info("{} Resuming stream completed", tag);
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
