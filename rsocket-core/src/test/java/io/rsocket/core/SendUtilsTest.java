package io.rsocket.core;

import static org.mockito.Mockito.*;

import io.netty.util.ReferenceCounted;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

public class SendUtilsTest {

  @Test
  void droppedElementsConsumerShouldAcceptOtherTypesThanReferenceCounted() {
    Consumer value = extractDroppedElementConsumer();
    value.accept(new Object());
  }

  @Test
  void droppedElementsConsumerReleaseReference() {
    ReferenceCounted referenceCounted = mock(ReferenceCounted.class);
    when(referenceCounted.release()).thenReturn(true);

    Consumer value = extractDroppedElementConsumer();
    value.accept(referenceCounted);

    verify(referenceCounted).release();
  }

  private static Consumer<?> extractDroppedElementConsumer() {
    return (Consumer<?>) SendUtils.DISCARD_CONTEXT.stream().findAny().get().getValue();
  }
}
