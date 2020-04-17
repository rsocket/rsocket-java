package io.rsocket.util;

import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ByteBufPayloadTest {

  @Test
  public void shouldIndicateThatItHasMetadata() {
    Payload payload = ByteBufPayload.create("data", "metadata");

    Assertions.assertThat(payload.hasMetadata()).isTrue();
    Assertions.assertThat(payload.release()).isTrue();
  }

  @Test
  public void shouldIndicateThatItHasNotMetadata() {
    Payload payload = ByteBufPayload.create("data");

    Assertions.assertThat(payload.hasMetadata()).isFalse();
    Assertions.assertThat(payload.release()).isTrue();
  }

  @Test
  public void shouldIndicateThatItHasNotMetadata1() {
    Payload payload =
        ByteBufPayload.create(Unpooled.wrappedBuffer("data".getBytes()), Unpooled.EMPTY_BUFFER);

    Assertions.assertThat(payload.hasMetadata()).isFalse();
    Assertions.assertThat(payload.release()).isTrue();
  }

  @Test
  public void shouldThrowExceptionIfAccessAfterRelease() {
    Payload payload = ByteBufPayload.create("data", "metadata");

    Assertions.assertThat(payload.release()).isTrue();

    Assertions.assertThatThrownBy(payload::hasMetadata)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::data).isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::metadata)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::sliceData)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::sliceMetadata)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::touch)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(() -> payload.touch("test"))
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::getData)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::getMetadata)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::getDataUtf8)
        .isInstanceOf(IllegalReferenceCountException.class);
    Assertions.assertThatThrownBy(payload::getMetadataUtf8)
        .isInstanceOf(IllegalReferenceCountException.class);
  }
}
