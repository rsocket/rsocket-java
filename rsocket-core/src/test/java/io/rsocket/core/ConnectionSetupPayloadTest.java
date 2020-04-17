package io.rsocket.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Test;

class ConnectionSetupPayloadTest {
  private static final int KEEP_ALIVE_INTERVAL = 5;
  private static final int KEEP_ALIVE_MAX_LIFETIME = 500;
  private static final String METADATA_TYPE = "metadata_type";
  private static final String DATA_TYPE = "data_type";

  @Test
  void testSetupPayloadWithDataMetadata() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {2, 1, 0});
    Payload payload = DefaultPayload.create(data, metadata);
    boolean leaseEnabled = true;

    ByteBuf frame = encodeSetupFrame(leaseEnabled, payload);
    ConnectionSetupPayload setupPayload = new DefaultConnectionSetupPayload(frame);

    assertTrue(setupPayload.willClientHonorLease());
    assertEquals(KEEP_ALIVE_INTERVAL, setupPayload.keepAliveInterval());
    assertEquals(KEEP_ALIVE_MAX_LIFETIME, setupPayload.keepAliveMaxLifetime());
    assertEquals(METADATA_TYPE, SetupFrameFlyweight.metadataMimeType(frame));
    assertEquals(DATA_TYPE, SetupFrameFlyweight.dataMimeType(frame));
    assertTrue(setupPayload.hasMetadata());
    assertNotNull(setupPayload.metadata());
    assertEquals(payload.metadata(), setupPayload.metadata());
    assertEquals(payload.data(), setupPayload.data());
    frame.release();
  }

  @Test
  void testSetupPayloadWithNoMetadata() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf metadata = null;
    Payload payload = DefaultPayload.create(data, metadata);
    boolean leaseEnabled = false;

    ByteBuf frame = encodeSetupFrame(leaseEnabled, payload);
    ConnectionSetupPayload setupPayload = new DefaultConnectionSetupPayload(frame);

    assertFalse(setupPayload.willClientHonorLease());
    assertFalse(setupPayload.hasMetadata());
    assertNotNull(setupPayload.metadata());
    assertEquals(0, setupPayload.metadata().readableBytes());
    assertEquals(payload.data(), setupPayload.data());
    frame.release();
  }

  @Test
  void testSetupPayloadWithEmptyMetadata() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf metadata = Unpooled.EMPTY_BUFFER;
    Payload payload = DefaultPayload.create(data, metadata);
    boolean leaseEnabled = false;

    ByteBuf frame = encodeSetupFrame(leaseEnabled, payload);
    ConnectionSetupPayload setupPayload = new DefaultConnectionSetupPayload(frame);

    assertFalse(setupPayload.willClientHonorLease());
    assertFalse(setupPayload.hasMetadata());
    assertNotNull(setupPayload.metadata());
    assertEquals(0, setupPayload.metadata().readableBytes());
    assertEquals(payload.data(), setupPayload.data());
    frame.release();
  }

  private static ByteBuf encodeSetupFrame(boolean leaseEnabled, Payload setupPayload) {
    return SetupFrameFlyweight.encode(
        ByteBufAllocator.DEFAULT,
        leaseEnabled,
        KEEP_ALIVE_INTERVAL,
        KEEP_ALIVE_MAX_LIFETIME,
        null,
        METADATA_TYPE,
        DATA_TYPE,
        setupPayload);
  }
}
