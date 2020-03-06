package io.rsocket;

import static org.assertj.core.error.ShouldBe.shouldBe;
import static org.assertj.core.error.ShouldBeEqual.shouldBeEqual;
import static org.assertj.core.error.ShouldHave.shouldHave;
import static org.assertj.core.error.ShouldNotHave.shouldNotHave;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.frame.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.Objects;
import reactor.util.annotation.Nullable;

public class FrameAssert extends AbstractAssert<FrameAssert, ByteBuf> {
  public static FrameAssert assertThat(@Nullable ByteBuf frame) {
    return new FrameAssert(frame);
  }

  private final Failures failures = Failures.instance();

  public FrameAssert(@Nullable ByteBuf frame) {
    super(frame, FrameAssert.class);
  }

  public FrameAssert hasMetadata() {
    assertValid();

    if (!FrameHeaderCodec.hasMetadata(actual)) {
      throw failures.failure(info, shouldHave(actual, new Condition<>("metadata present")));
    }

    return this;
  }

  public FrameAssert hasNoMetadata() {
    assertValid();

    if (FrameHeaderCodec.hasMetadata(actual)) {
      throw failures.failure(info, shouldHave(actual, new Condition<>("metadata absent")));
    }

    return this;
  }

  public FrameAssert hasMetadata(String metadata, Charset charset) {
    return hasMetadata(metadata.getBytes(charset));
  }

  public FrameAssert hasMetadata(String metadataUtf8) {
    return hasMetadata(metadataUtf8, CharsetUtil.UTF_8);
  }

  public FrameAssert hasMetadata(byte[] metadata) {
    return hasMetadata(Unpooled.wrappedBuffer(metadata));
  }

  public FrameAssert hasMetadata(ByteBuf metadata) {
    hasMetadata();

    final FrameType frameType = FrameHeaderCodec.frameType(actual);
    ByteBuf content;
    if (frameType == FrameType.METADATA_PUSH) {
      content = MetadataPushFrameCodec.metadata(actual);
    } else if (frameType.hasInitialRequestN()) {
      content = RequestStreamFrameCodec.metadata(actual);
    } else {
      content = PayloadFrameCodec.metadata(actual);
    }

    if (!ByteBufUtil.equals(content, metadata)) {
      throw failures.failure(info, shouldBeEqual(content, metadata, new ByteBufRepresentation()));
    }

    return this;
  }

  public FrameAssert hasData(String dataUtf8) {
    return hasData(dataUtf8, CharsetUtil.UTF_8);
  }

  public FrameAssert hasData(String data, Charset charset) {
    return hasData(data.getBytes(charset));
  }

  public FrameAssert hasData(byte[] data) {
    return hasData(Unpooled.wrappedBuffer(data));
  }

  public FrameAssert hasData(ByteBuf data) {
    assertValid();

    ByteBuf content;
    final FrameType frameType = FrameHeaderCodec.frameType(actual);
    if (!frameType.canHaveData()) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have data content but frame type  %n<%s> does not support data content",
              actual, frameType));
    } else if (frameType.hasInitialRequestN()) {
      content = RequestStreamFrameCodec.data(actual);
    } else if (frameType == FrameType.ERROR) {
      content = ErrorFrameCodec.data(actual);
    } else {
      content = PayloadFrameCodec.data(actual);
    }

    if (!ByteBufUtil.equals(content, data)) {
      throw failures.failure(info, shouldBeEqual(content, data, new ByteBufRepresentation()));
    }

    return this;
  }

  public FrameAssert hasFragmentsFollow() {
    return hasFollows(true);
  }

  public FrameAssert hasNoFragmentsFollow() {
    return hasFollows(false);
  }

  public FrameAssert hasFollows(boolean hasFollows) {
    assertValid();

    if (FrameHeaderCodec.hasFollows(actual) != hasFollows) {
      throw failures.failure(
          info,
          hasFollows
              ? shouldHave(actual, new Condition<>("follows fragment present"))
              : shouldNotHave(actual, new Condition<>("follows fragment present")));
    }

    return this;
  }

  public FrameAssert typeOf(FrameType frameType) {
    assertValid();

    final FrameType currentFrameType = FrameHeaderCodec.frameType(actual);
    if (currentFrameType != frameType) {
      throw failures.failure(
          info, shouldBe(currentFrameType, new Condition<>("frame of type [" + frameType + "]")));
    }

    return this;
  }

  public FrameAssert hasStreamId(int streamId) {
    assertValid();

    final int currentStreamId = FrameHeaderCodec.streamId(actual);
    if (currentStreamId != streamId) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting streamId:%n<%s>%n to be equal %n<%s>", currentStreamId, streamId));
    }

    return this;
  }

  public FrameAssert hasStreamIdZero() {
    return hasStreamId(0);
  }

  public FrameAssert hasClientSideStreamId() {
    assertValid();

    final int currentStreamId = FrameHeaderCodec.streamId(actual);
    if (currentStreamId % 2 != 1) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting Client Side StreamId %nbut was "
                  + (currentStreamId == 0 ? "Stream Id 0" : "Server Side Stream Id")));
    }

    return this;
  }

  public FrameAssert hasServerSideStreamId() {
    assertValid();

    final int currentStreamId = FrameHeaderCodec.streamId(actual);
    if (currentStreamId == 0 || currentStreamId % 2 != 0) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting %n  Server Side Stream Id %nbut was %n  "
                  + (currentStreamId == 0 ? "Stream Id 0" : "Client Side Stream Id")));
    }

    return this;
  }

  public FrameAssert hasPayloadSize(int payloadLength) {
    assertValid();

    final FrameType currentFrameType = FrameHeaderCodec.frameType(actual);

    final int currentFrameLength =
        actual.readableBytes()
            - FrameHeaderCodec.size()
            - (FrameHeaderCodec.hasMetadata(actual) && currentFrameType.canHaveData() ? 3 : 0)
            - (currentFrameType.hasInitialRequestN() ? Integer.BYTES : 0);
    if (currentFrameLength != payloadLength) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting %n<%s> %nframe payload size to be equal to  %n<%s>  %nbut was  %n<%s>",
              actual, payloadLength, currentFrameLength));
    }

    return this;
  }

  public FrameAssert hasRequestN(int n) {
    assertValid();

    final FrameType currentFrameType = FrameHeaderCodec.frameType(actual);
    long requestN;
    if (currentFrameType.hasInitialRequestN()) {
      requestN = RequestStreamFrameCodec.initialRequestN(actual);
    } else if (currentFrameType == FrameType.REQUEST_N) {
      requestN = RequestNFrameCodec.requestN(actual);
    } else {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have requestN but frame type  %n<%s> does not support requestN",
              actual, currentFrameType));
    }

    if ((requestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : requestN) != n) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have  %nrequestN(<%s>)  but got  %nrequestN(<%s>)",
              actual, n, requestN));
    }

    return this;
  }

  public FrameAssert hasPayload(Payload expectedPayload) {
    assertValid();

    List<String> failedExpectation = new ArrayList<>();
    FrameType frameType = FrameHeaderCodec.frameType(actual);
    boolean hasMetadata = FrameHeaderCodec.hasMetadata(actual);
    if (expectedPayload.hasMetadata() != hasMetadata) {
      failedExpectation.add(
          String.format(
              "hasMetadata(%s) but actual was hasMetadata(%s)%n",
              expectedPayload.hasMetadata(), hasMetadata));
    } else if (hasMetadata) {
      ByteBuf metadataContent;
      if (frameType == FrameType.METADATA_PUSH) {
        metadataContent = MetadataPushFrameCodec.metadata(actual);
      } else if (frameType.hasInitialRequestN()) {
        metadataContent = RequestStreamFrameCodec.metadata(actual);
      } else {
        metadataContent = PayloadFrameCodec.metadata(actual);
      }
      if (!ByteBufUtil.equals(expectedPayload.sliceMetadata(), metadataContent)) {
        failedExpectation.add(
            String.format(
                "metadata(%s) but actual was metadata(%s)%n",
                expectedPayload.sliceMetadata(), metadataContent));
      }
    }

    ByteBuf dataContent;
    if (!frameType.canHaveData() && expectedPayload.sliceData().readableBytes() > 0) {
      failedExpectation.add(
          String.format(
              "data(%s) but frame type  %n<%s> does not support data", actual, frameType));
    } else {
      if (frameType.hasInitialRequestN()) {
        dataContent = RequestStreamFrameCodec.data(actual);
      } else {
        dataContent = PayloadFrameCodec.data(actual);
      }

      if (!ByteBufUtil.equals(expectedPayload.sliceData(), dataContent)) {
        failedExpectation.add(
            String.format(
                "data(%s) but actual was data(%s)%n", expectedPayload.sliceData(), dataContent));
      }
    }

    if (!failedExpectation.isEmpty()) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting be equal to the given payload but the following differences were found"
                  + " %s",
              failedExpectation));
    }

    return this;
  }

  public void hasNoLeaks() {
    if (!actual.release() || actual.refCnt() > 0) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have refCnt(0) after release but "
                  + "actual was "
                  + "%n<refCnt(%s)>",
              actual, actual.refCnt()));
    }
  }

  private void assertValid() {
    Objects.instance().assertNotNull(info, actual);

    try {
      FrameHeaderCodec.frameType(actual);
    } catch (Throwable t) {
      throw failures.failure(
          info, shouldBe(actual, new Condition<>("a valid frame, but got exception [" + t + "]")));
    }
  }
}
