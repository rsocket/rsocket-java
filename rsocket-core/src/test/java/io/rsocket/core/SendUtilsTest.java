package io.rsocket.core;

import io.netty.util.ReferenceCounted;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.mockito.Mockito.*;

class SendUtilsTest {

    @Test
    void droppedElementsConsumerShouldAcceptOtherTypesThanReferenceCounted() {
        Consumer value = extractDroppedElementConsumer();
        value.accept(new Object());
    }

    @Test
    void droppedElementsConsumerReleaseReference() {
        ReferenceCounted referenceCounted =  mock(ReferenceCounted.class);
        when(referenceCounted.release()).thenReturn(true);

        Consumer value = extractDroppedElementConsumer();
        value.accept(referenceCounted);

        verify(referenceCounted).release();
    }

    private static Consumer<?> extractDroppedElementConsumer() {
        return (Consumer<?>) SendUtils.DISCARD_CONTEXT.stream().findAny().get().getValue();
    }
}