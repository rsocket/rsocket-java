package io.rsocket;

import static org.junit.Assert.assertEquals;

import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.util.DefaultPayload;
import org.junit.Test;

public class FrameTest {
  @Test
  public void testFrameToString() {
    final Frame requestFrame =
        Frame.Request.from(
            1, FrameType.REQUEST_RESPONSE, DefaultPayload.create("streaming in -> 0"), 1);
    assertEquals(
        "Frame => Stream ID: 1 Type: REQUEST_RESPONSE Payload: data: \"streaming in -> 0\" ",
        requestFrame.toString());
  }

  @Test
  public void testFrameWithMetadataToString() {
    final Frame requestFrame =
        Frame.Request.from(
            1,
            FrameType.REQUEST_RESPONSE,
            DefaultPayload.create("streaming in -> 0", "metadata"),
            1);
    assertEquals(
        "Frame => Stream ID: 1 Type: REQUEST_RESPONSE Payload: metadata: \"metadata\" data: \"streaming in -> 0\" ",
        requestFrame.toString());
  }

  @Test
  public void testPayload() {
    Frame frame =
        Frame.PayloadFrame.from(
            1,
            FrameType.NEXT_COMPLETE,
            DefaultPayload.create("Hello"),
            FrameHeaderFlyweight.FLAGS_C);
    frame.toString();
  }
}
