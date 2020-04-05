package io.rsocket.fragmentation;

import static org.junit.jupiter.api.Assertions.*;

import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.util.DefaultPayload;
import java.util.concurrent.ThreadLocalRandom;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class FragmentationUtilsTest {

  @Test
  void shouldValidFrameWithNoFragmentation() {
    byte[] data =
        new byte
            [FrameLengthFlyweight.FRAME_LENGTH_MASK
                - FrameLengthFlyweight.FRAME_LENGTH_SIZE
                - FrameHeaderFlyweight.size()];
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data);

    Assertions.assertThat(FragmentationUtils.isValid(0, payload)).isTrue();
  }

  @Test
  void shouldValidFrameWithNoFragmentation0() {
    byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK / 2];
    byte[] data =
        new byte
            [FrameLengthFlyweight.FRAME_LENGTH_MASK / 2
                - FrameLengthFlyweight.FRAME_LENGTH_SIZE
                - FrameHeaderFlyweight.size()
                - FrameHeaderFlyweight.size()];
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(FragmentationUtils.isValid(0, payload)).isTrue();
  }

  @Test
  void shouldValidFrameWithNoFragmentation1() {
    byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(FragmentationUtils.isValid(0, payload)).isFalse();
  }

  @Test
  void shouldValidFrameWithNoFragmentation2() {
    byte[] metadata = new byte[1];
    byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(FragmentationUtils.isValid(0, payload)).isTrue();
  }

  @Test
  void shouldValidFrameWithNoFragmentation3() {
    byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(FragmentationUtils.isValid(64, payload)).isTrue();
  }

  @Test
  void shouldValidFrameWithNoFragmentation4() {
    byte[] metadata = new byte[1];
    byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);

    Assertions.assertThat(FragmentationUtils.isValid(64, payload)).isTrue();
  }
}
