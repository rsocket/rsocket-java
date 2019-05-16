package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeMetadataTest {

    @Test
    void decodeCompositeMetadata() {
        //metadata 1:
        WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
        ByteBuf metadata1 = ByteBufAllocator.DEFAULT.buffer();
        metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);

        //metadata 2:
        String mimeType2 = "application/custom";
        ByteBuf metadata2 = ByteBufAllocator.DEFAULT.buffer();
        metadata2.writeChar('E');
        metadata2.writeChar('∑');
        metadata2.writeChar('é');
        metadata2.writeBoolean(true);
        metadata2.writeChar('W');

        CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);

        CompositeMetadata metadata = CompositeMetadata.decode(compositeMetadata);

        assertThat(metadata.size()).as("size").isEqualTo(2);

        assertThat(metadata.get(0))
                .satisfies(e -> assertThat(e.getMimeType())
                        .as("metadata1 mime")
                        .isEqualTo(WellKnownMimeType.APPLICATION_PDF.getMime())
                )
                .satisfies(e -> assertThat(e.getMetadata().toString(CharsetUtil.UTF_8))
                        .as("metadata1 decoded")
                        .isEqualTo("abcdefghijkl")
                );

        System.out.println(ByteBufUtil.hexDump(metadata.get(1).getMetadata()));

        assertThat(metadata.get(1))
                .satisfies(e -> assertThat(e.getMimeType())
                        .as("metadata2 mime")
                        .isEqualTo(mimeType2)
                )
                .satisfies(e -> assertThat(e.getMetadata())
                        .as("metadata2 buffer")
                        .isEqualByComparingTo(metadata2)
                );
    }

    //TODO unit tests for get(String), get(int), getAll(String) and getAll() => read-only nature, out of bounds, etc...

}