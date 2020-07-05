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
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] data =
        new byte[maxFrameLength - FrameLengthCodec.FRAME_LENGTH_SIZE - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload, maxFrameLength)).isTrue();
  }

  @Test
  void shouldBeInValidFrameWithNoFragmentation() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] data =
        new byte[maxFrameLength - FrameLengthCodec.FRAME_LENGTH_SIZE - FrameHeaderCodec.size() + 1];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload, maxFrameLength)).isFalse();
  }

  @Test
  void shouldBeValidFrameWithNoFragmentation0() {
    int maxFrameLength =
        ThreadLocalRandom.current().nextInt(64, FrameLengthCodec.FRAME_LENGTH_MASK);
    byte[] metadata = new byte[maxFrameLength / 2];
    byte[] data =
        new byte
            [maxFrameLength / 2
                - FrameLengthCodec.FRAME_LENGTH_SIZE
                - FrameHeaderCodec.size()
                - FrameHeaderCodec.size()];
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload, maxFrameLength)).isTrue();
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

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload, maxFrameLength)).isFalse();
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

    Assertions.assertThat(PayloadValidationUtils.isValid(0, payload, maxFrameLength)).isTrue();
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

    Assertions.assertThat(PayloadValidationUtils.isValid(64, payload, maxFrameLength)).isTrue();
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

    Assertions.assertThat(PayloadValidationUtils.isValid(64, payload, maxFrameLength)).isTrue();
  }
}
