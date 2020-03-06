package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_SIZE;

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
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] data = new byte[maxFrameLength - FRAME_LENGTH_SIZE - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, false))
        .isTrue();
    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, true))
        .isFalse();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation1() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] data =
        new byte[maxFrameLength - FRAME_LENGTH_SIZE - Integer.BYTES - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, false))
        .isTrue();
    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, true))
        .isTrue();
  }

  @Test
  void shouldBeInValidFrameWithNoFragmentation() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] data = new byte[maxFrameLength - FRAME_LENGTH_SIZE - FrameHeaderCodec.size() + 1];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, true))
        .isFalse();
    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, false))
        .isFalse();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation0() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] metadata = new byte[maxFrameLength / 2];
    byte[] data =
        new byte
            [(maxFrameLength / 2 + 1)
                - FRAME_LENGTH_SIZE
                - FrameHeaderCodec.size()
                - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, false))
        .isTrue();
    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, true))
        .isFalse();
  }

  @Test
  void shouldBeInValidFrameWithNoFragmentation1() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] metadata = new byte[maxFrameLength];
    byte[] data = new byte[maxFrameLength];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, true))
        .isFalse();
    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, false))
        .isFalse();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation2() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] metadata = new byte[1];
    byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, true))
        .isTrue();
    Assertions.assertThat(PayloadValidationUtils.isValid(0, maxFrameLength, payload, false))
        .isTrue();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation3() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] metadata = new byte[maxFrameLength];
    byte[] data = new byte[maxFrameLength];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(64, maxFrameLength, payload, true))
        .isTrue();
    Assertions.assertThat(PayloadValidationUtils.isValid(64, maxFrameLength, payload, false))
        .isTrue();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation4() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] metadata = new byte[1];
    byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(64, maxFrameLength, payload, true))
        .isTrue();
    Assertions.assertThat(PayloadValidationUtils.isValid(64, maxFrameLength, payload, false))
        .isTrue();
  }
}
