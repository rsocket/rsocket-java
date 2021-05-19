package io.rsocket.resume;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class InMemoryResumeStoreTest {

  @Test
  void saveNonResumableFrame() {
    InMemoryResumableFramesStore store = inMemoryStore(25);
    ByteBuf frame1 = fakeConnectionFrame(10);
    ByteBuf frame2 = fakeConnectionFrame(35);
    store.saveFrames(Flux.just(frame1, frame2)).block();
    assertThat(store.cachedFrames.size()).isZero();
    assertThat(store.cacheSize).isZero();
    assertThat(store.position).isZero();
    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
  }

  @Test
  void saveWithoutTailRemoval() {
    InMemoryResumableFramesStore store = inMemoryStore(25);
    ByteBuf frame = fakeResumableFrame(10);
    store.saveFrames(Flux.just(frame)).block();
    assertThat(store.cachedFrames.size()).isEqualTo(1);
    assertThat(store.cacheSize).isEqualTo(frame.readableBytes());
    assertThat(store.position).isZero();
    assertThat(frame.refCnt()).isOne();
  }

  @Test
  void saveRemoveOneFromTail() {
    InMemoryResumableFramesStore store = inMemoryStore(25);
    ByteBuf frame1 = fakeResumableFrame(20);
    ByteBuf frame2 = fakeResumableFrame(10);
    store.saveFrames(Flux.just(frame1, frame2)).block();
    assertThat(store.cachedFrames.size()).isOne();
    assertThat(store.cacheSize).isEqualTo(frame2.readableBytes());
    assertThat(store.position).isEqualTo(frame1.readableBytes());
    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isOne();
  }

  @Test
  void saveRemoveTwoFromTail() {
    InMemoryResumableFramesStore store = inMemoryStore(25);
    ByteBuf frame1 = fakeResumableFrame(10);
    ByteBuf frame2 = fakeResumableFrame(10);
    ByteBuf frame3 = fakeResumableFrame(20);
    store.saveFrames(Flux.just(frame1, frame2, frame3)).block();
    assertThat(store.cachedFrames.size()).isOne();
    assertThat(store.cacheSize).isEqualTo(frame3.readableBytes());
    assertThat(store.position).isEqualTo(size(frame1, frame2));
    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isOne();
  }

  @Test
  void saveBiggerThanStore() {
    InMemoryResumableFramesStore store = inMemoryStore(25);
    ByteBuf frame1 = fakeResumableFrame(10);
    ByteBuf frame2 = fakeResumableFrame(10);
    ByteBuf frame3 = fakeResumableFrame(30);
    store.saveFrames(Flux.just(frame1, frame2, frame3)).block();
    assertThat(store.cachedFrames.size()).isZero();
    assertThat(store.cacheSize).isZero();
    assertThat(store.position).isEqualTo(size(frame1, frame2, frame3));
    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isZero();
  }

  @Test
  void releaseFrames() {
    InMemoryResumableFramesStore store = inMemoryStore(100);
    ByteBuf frame1 = fakeResumableFrame(10);
    ByteBuf frame2 = fakeResumableFrame(10);
    ByteBuf frame3 = fakeResumableFrame(30);
    store.saveFrames(Flux.just(frame1, frame2, frame3)).block();
    store.releaseFrames(20);
    assertThat(store.cachedFrames.size()).isOne();
    assertThat(store.cacheSize).isEqualTo(frame3.readableBytes());
    assertThat(store.position).isEqualTo(size(frame1, frame2));
    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isOne();
  }

  @Test
  void receiveImpliedPosition() {
    InMemoryResumableFramesStore store = inMemoryStore(100);
    ByteBuf frame1 = fakeResumableFrame(10);
    ByteBuf frame2 = fakeResumableFrame(30);
    store.resumableFrameReceived(frame1);
    store.resumableFrameReceived(frame2);
    assertThat(store.frameImpliedPosition()).isEqualTo(size(frame1, frame2));
  }

  private int size(ByteBuf... byteBufs) {
    return Arrays.stream(byteBufs).mapToInt(ByteBuf::readableBytes).sum();
  }

  private static InMemoryResumableFramesStore inMemoryStore(int size) {
    return new InMemoryResumableFramesStore("test", size);
  }

  private static ByteBuf fakeResumableFrame(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 7);
    return Unpooled.wrappedBuffer(bytes);
  }

  private static ByteBuf fakeConnectionFrame(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 0);
    return Unpooled.wrappedBuffer(bytes);
  }
}
