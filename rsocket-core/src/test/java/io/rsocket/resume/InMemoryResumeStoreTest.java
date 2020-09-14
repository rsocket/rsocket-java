// package io.rsocket.resume;
//
// import io.netty.buffer.ByteBuf;
// import io.netty.buffer.Unpooled;
// import java.util.Arrays;
// import org.junit.Assert;
// import org.junit.jupiter.api.Test;
// import reactor.core.publisher.Flux;
//
// public class InMemoryResumeStoreTest {
//
//  @Test
//  void saveWithoutTailRemoval() {
//    InMemoryResumableFramesStore store = inMemoryStore(25);
//    ByteBuf frame = frameMock(10);
//    store.saveFrames(Flux.just(frame)).block();
//    Assert.assertEquals(1, store.cachedFrames.size());
//    Assert.assertEquals(frame.readableBytes(), store.cacheSize);
//    Assert.assertEquals(0, store.position);
//  }
//
//  @Test
//  void saveRemoveOneFromTail() {
//    InMemoryResumableFramesStore store = inMemoryStore(25);
//    ByteBuf frame1 = frameMock(20);
//    ByteBuf frame2 = frameMock(10);
//    store.saveFrames(Flux.just(frame1, frame2)).block();
//    Assert.assertEquals(1, store.cachedFrames.size());
//    Assert.assertEquals(frame2.readableBytes(), store.cacheSize);
//    Assert.assertEquals(frame1.readableBytes(), store.position);
//  }
//
//  @Test
//  void saveRemoveTwoFromTail() {
//    InMemoryResumableFramesStore store = inMemoryStore(25);
//    ByteBuf frame1 = frameMock(10);
//    ByteBuf frame2 = frameMock(10);
//    ByteBuf frame3 = frameMock(20);
//    store.saveFrames(Flux.just(frame1, frame2, frame3)).block();
//    Assert.assertEquals(1, store.cachedFrames.size());
//    Assert.assertEquals(frame3.readableBytes(), store.cacheSize);
//    Assert.assertEquals(size(frame1, frame2), store.position);
//  }
//
//  @Test
//  void saveBiggerThanStore() {
//    InMemoryResumableFramesStore store = inMemoryStore(25);
//    ByteBuf frame1 = frameMock(10);
//    ByteBuf frame2 = frameMock(10);
//    ByteBuf frame3 = frameMock(30);
//    store.saveFrames(Flux.just(frame1, frame2, frame3)).block();
//    Assert.assertEquals(0, store.cachedFrames.size());
//    Assert.assertEquals(0, store.cacheSize);
//    Assert.assertEquals(size(frame1, frame2, frame3), store.position);
//  }
//
//  @Test
//  void releaseFrames() {
//    InMemoryResumableFramesStore store = inMemoryStore(100);
//    ByteBuf frame1 = frameMock(10);
//    ByteBuf frame2 = frameMock(10);
//    ByteBuf frame3 = frameMock(30);
//    store.saveFrames(Flux.just(frame1, frame2, frame3)).block();
//    store.releaseFrames(20);
//    Assert.assertEquals(1, store.cachedFrames.size());
//    Assert.assertEquals(frame3.readableBytes(), store.cacheSize);
//    Assert.assertEquals(size(frame1, frame2), store.position);
//  }
//
//  @Test
//  void receiveImpliedPosition() {
//    InMemoryResumableFramesStore store = inMemoryStore(100);
//    ByteBuf frame1 = frameMock(10);
//    ByteBuf frame2 = frameMock(30);
//    store.resumableFrameReceived(frame1);
//    store.resumableFrameReceived(frame2);
//    Assert.assertEquals(size(frame1, frame2), store.frameImpliedPosition());
//  }
//
//  private int size(ByteBuf... byteBufs) {
//    return Arrays.stream(byteBufs).mapToInt(ByteBuf::readableBytes).sum();
//  }
//
//  private static InMemoryResumableFramesStore inMemoryStore(int size) {
//    return new InMemoryResumableFramesStore("test", size);
//  }
//
//  private static ByteBuf frameMock(int size) {
//    byte[] bytes = new byte[size];
//    Arrays.fill(bytes, (byte) 7);
//    return Unpooled.wrappedBuffer(bytes);
//  }
// }
