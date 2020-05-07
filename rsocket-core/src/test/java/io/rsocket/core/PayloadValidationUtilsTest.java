package io.rsocket.core;

import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.util.DefaultPayload;
import java.util.concurrent.ThreadLocalRandom;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class PayloadValidationUtilsTest {

  @Test
  void shouldBeValidFrameWithNoFragmentation() {
    byte[] data =
        new byte
            [FrameLengthCodec.FRAME_LENGTH_MASK
                - FrameLengthCodec.FRAME_LENGTH_SIZE
                - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload)).isTrue();
  }

  @Test
  void shouldBeInValidFrameWithNoFragmentation() {
    byte[] data =
        new byte
            [FrameLengthCodec.FRAME_LENGTH_MASK
                - FrameLengthCodec.FRAME_LENGTH_SIZE
                - FrameHeaderCodec.size()
                + 1];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload)).isFalse();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation0() {
    byte[] metadata = new byte[FrameLengthCodec.FRAME_LENGTH_MASK / 2];
    byte[] data =
        new byte
            [FrameLengthCodec.FRAME_LENGTH_MASK / 2
                - FrameLengthCodec.FRAME_LENGTH_SIZE
                - FrameHeaderCodec.size()
                - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload)).isTrue();
  }

  @Test
  void shouldBeInValidFrameWithNoFragmentation1() {
    byte[] metadata = new byte[FrameLengthCodec.FRAME_LENGTH_MASK];
    byte[] data = new byte[FrameLengthCodec.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload)).isFalse();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation2() {
    byte[] metadata = new byte[1];
    byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload)).isTrue();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation3() {
    byte[] metadata = new byte[FrameLengthCodec.FRAME_LENGTH_MASK];
    byte[] data = new byte[FrameLengthCodec.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(64, payload)).isTrue();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation4() {
    byte[] metadata = new byte[1];
    byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(64, payload)).isTrue();
  }
}
