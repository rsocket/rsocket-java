package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class PayloadFlyweightTest {

  @Test
  void nextCompleteDataMetadata() {
    Payload payload = DefaultPayload.create("d", "md");
    ByteBuf nextComplete = PayloadFrameFlyweight.encodeNextComplete(ByteBufAllocator.DEFAULT, 1, payload);
    String data = PayloadFrameFlyweight.data(nextComplete).toString(StandardCharsets.UTF_8);
    String metadata = PayloadFrameFlyweight.metadata(nextComplete).toString(StandardCharsets.UTF_8);
    Assertions.assertEquals("d", data);
    Assertions.assertEquals("md", metadata);
  }

  @Test
  void nextCompleteData() {
    Payload payload = DefaultPayload.create("d");
    ByteBuf nextComplete = PayloadFrameFlyweight.encodeNextComplete(ByteBufAllocator.DEFAULT, 1, payload);
    String data = PayloadFrameFlyweight.data(nextComplete).toString(StandardCharsets.UTF_8);
    ByteBuf metadata = PayloadFrameFlyweight.metadata(nextComplete);
    Assertions.assertEquals("d", data);
    Assertions.assertTrue(metadata.readableBytes() == 0);
  }

  @Test
  void nextCompleteMetaData() {
    Payload payload = DefaultPayload.create(
        Unpooled.EMPTY_BUFFER,
        Unpooled.wrappedBuffer("md".getBytes(StandardCharsets.UTF_8)));

    ByteBuf nextComplete = PayloadFrameFlyweight.encodeNextComplete(ByteBufAllocator.DEFAULT, 1, payload);
    ByteBuf data = PayloadFrameFlyweight.data(nextComplete);
    String metadata = PayloadFrameFlyweight.metadata(nextComplete).toString(StandardCharsets.UTF_8);
    Assertions.assertTrue(data.readableBytes() == 0);
    Assertions.assertEquals("md", metadata);
  }

  @Test
  void nextDataMetadata() {
    Payload payload = DefaultPayload.create("d", "md");
    ByteBuf next = PayloadFrameFlyweight.encodeNext(ByteBufAllocator.DEFAULT, 1, payload);
    String data = PayloadFrameFlyweight.data(next).toString(StandardCharsets.UTF_8);
    String metadata = PayloadFrameFlyweight.metadata(next).toString(StandardCharsets.UTF_8);
    Assertions.assertEquals("d", data);
    Assertions.assertEquals("md", metadata);
  }

  @Test
  void nextData() {
    Payload payload = DefaultPayload.create("d");
    ByteBuf next = PayloadFrameFlyweight.encodeNext(ByteBufAllocator.DEFAULT, 1, payload);
    String data = PayloadFrameFlyweight.data(next).toString(StandardCharsets.UTF_8);
    ByteBuf metadata = PayloadFrameFlyweight.metadata(next);
    Assertions.assertEquals("d", data);
    Assertions.assertTrue(metadata.readableBytes() == 0);
  }
}
