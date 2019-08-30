package io.rsocket.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBufAllocator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tagging metadata test
 *
 * @author linux_china
 */
public class TaggingMetadataTest {
  private ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;

  @Test
  public void testParseTags() {
    List<String> tags =
        Arrays.asList(
            "ws://localhost:8080/rsocket", String.join("", Collections.nCopies(129, "x")));
    TaggingMetadata taggingMetadata =
        TaggingMetadataFlyweight.createTaggingMetadata(
            byteBufAllocator, "message/x.rsocket.routing.v0", tags);
    TaggingMetadata taggingMetadataCopy =
        new TaggingMetadata("message/x.rsocket.routing.v0", taggingMetadata.getContent());
    assertThat(tags)
        .containsExactlyElementsOf(taggingMetadataCopy.stream().collect(Collectors.toList()));
  }

  @Test
  public void testEmptyTagAndOverLengthTag() {
    List<String> tags =
        Arrays.asList(
            "ws://localhost:8080/rsocket", "", String.join("", Collections.nCopies(256, "x")));
    TaggingMetadata taggingMetadata =
        TaggingMetadataFlyweight.createTaggingMetadata(
            byteBufAllocator, "message/x.rsocket.routing.v0", tags);
    TaggingMetadata taggingMetadataCopy =
        new TaggingMetadata("message/x.rsocket.routing.v0", taggingMetadata.getContent());
    assertThat(tags.subList(0, 1))
        .containsExactlyElementsOf(taggingMetadataCopy.stream().collect(Collectors.toList()));
  }
}
