package io.rsocket.resume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.util.PayloadImpl;
import org.junit.jupiter.api.Test;

public class ResumeUtilTest {
  private final Frame CANCEL = Frame.Cancel.from(1);
  private final Frame STREAM =
      Frame.Request.from(1, FrameType.REQUEST_STREAM, new PayloadImpl("Test"), 100);

  @Test
  public void testSupportedTypes() {
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_STREAM));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_CHANNEL));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_RESPONSE));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_N));
    assertTrue(ResumeUtil.isTracked(FrameType.CANCEL));
    assertTrue(ResumeUtil.isTracked(FrameType.ERROR));
    assertTrue(ResumeUtil.isTracked(FrameType.FIRE_AND_FORGET));
    assertTrue(ResumeUtil.isTracked(FrameType.PAYLOAD));
  }

  @Test
  public void testUnsupportedTypes() {
    assertFalse(ResumeUtil.isTracked(FrameType.METADATA_PUSH));
    assertFalse(ResumeUtil.isTracked(FrameType.RESUME));
    assertFalse(ResumeUtil.isTracked(FrameType.RESUME_OK));
    assertFalse(ResumeUtil.isTracked(FrameType.SETUP));
    assertFalse(ResumeUtil.isTracked(FrameType.EXT));
    assertFalse(ResumeUtil.isTracked(FrameType.KEEPALIVE));
  }

  @Test
  public void testOffset() {
    assertEquals(6, ResumeUtil.offset(CANCEL));
    assertEquals(14, ResumeUtil.offset(STREAM));
  }
}
