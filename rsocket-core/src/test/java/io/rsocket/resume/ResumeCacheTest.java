package io.rsocket.resume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.util.PayloadImpl;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class ResumeCacheTest {
  private final Frame CANCEL = Frame.Cancel.from(1);
  private final Frame STREAM =
      Frame.Request.from(1, FrameType.REQUEST_STREAM, new PayloadImpl("Test"), 100);

  private ResumeCache cache = new ResumeCache(ResumePositionCounter.frames(), 2);

  @Test
  public void startsEmpty() {
    Flux<Frame> x = cache.resend(0);
    assertEquals(0L, (long) x.count().block());
    cache.updateRemotePosition(0);
  }

  @Test
  public void failsForFutureUpdatePosition() {
    assertThrows(IllegalStateException.class, () -> cache.updateRemotePosition(1));
  }

  @Test
  public void failsForFutureResend() {
    assertThrows(IllegalStateException.class, () -> cache.resend(1));
  }

  @Test
  public void updatesPositions() {
    assertEquals(0, cache.getRemotePosition());
    assertEquals(0, cache.getCurrentPosition());
    assertEquals(0, cache.getEarliestResendPosition());
    assertEquals(0, cache.size());

    cache.sent(STREAM);

    assertEquals(0, cache.getRemotePosition());
    assertEquals(14, cache.getCurrentPosition());
    assertEquals(0, cache.getEarliestResendPosition());
    assertEquals(1, cache.size());

    cache.updateRemotePosition(14);

    assertEquals(14, cache.getRemotePosition());
    assertEquals(14, cache.getCurrentPosition());
    assertEquals(14, cache.getEarliestResendPosition());
    assertEquals(0, cache.size());

    cache.sent(CANCEL);

    assertEquals(14, cache.getRemotePosition());
    assertEquals(20, cache.getCurrentPosition());
    assertEquals(14, cache.getEarliestResendPosition());
    assertEquals(1, cache.size());

    cache.updateRemotePosition(20);

    assertEquals(20, cache.getRemotePosition());
    assertEquals(20, cache.getCurrentPosition());
    assertEquals(20, cache.getEarliestResendPosition());
    assertEquals(0, cache.size());

    cache.sent(STREAM);

    assertEquals(20, cache.getRemotePosition());
    assertEquals(34, cache.getCurrentPosition());
    assertEquals(20, cache.getEarliestResendPosition());
    assertEquals(1, cache.size());
  }

  @Test
  public void supportsZeroBuffer() {
    cache = new ResumeCache(ResumePositionCounter.frames(), 0);

    cache.sent(STREAM);
    cache.sent(STREAM);
    cache.sent(STREAM);

    assertEquals(0, cache.getRemotePosition());
    assertEquals(42, cache.getCurrentPosition());
    assertEquals(42, cache.getEarliestResendPosition());
    assertEquals(0, cache.size());
  }

  @Test
  public void supportsFrameCountBuffers() {
    cache = new ResumeCache(ResumePositionCounter.size(), 100);

    assertEquals(0, cache.getRemotePosition());
    assertEquals(0, cache.getCurrentPosition());
    assertEquals(0, cache.getEarliestResendPosition());
    assertEquals(0, cache.size());

    cache.sent(STREAM);

    assertEquals(0, cache.getRemotePosition());
    assertEquals(14, cache.getCurrentPosition());
    assertEquals(0, cache.getEarliestResendPosition());
    assertEquals(14, cache.size());

    cache.updateRemotePosition(14);

    assertEquals(14, cache.getRemotePosition());
    assertEquals(14, cache.getCurrentPosition());
    assertEquals(14, cache.getEarliestResendPosition());
    assertEquals(0, cache.size());

    cache.sent(CANCEL);

    assertEquals(14, cache.getRemotePosition());
    assertEquals(20, cache.getCurrentPosition());
    assertEquals(14, cache.getEarliestResendPosition());
    assertEquals(6, cache.size());
  }
}
