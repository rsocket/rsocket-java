/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.resume;

public class ResumeCacheTest {
  /*private Frame CANCEL = Frame.Cancel.from(1);
  private Frame STREAM =
      Frame.Request.from(1, FrameType.REQUEST_STREAM, DefaultPayload.create("Test"), 100);

  private ResumeCache cache = new ResumeCache(ResumePositionCounter.frames(), 2);

  @Test
  public void startsEmpty() {
    Flux<Frame> x = cache.resend(0);
    assertEquals(0L, (long) x.count().block());
    cache.updateRemotePosition(0);
  }

  @Test(expected = IllegalStateException.class)
  public void failsForFutureUpdatePosition() {
    cache.updateRemotePosition(1);
  }

  @Test(expected = IllegalStateException.class)
  public void failsForFutureResend() {
    cache.resend(1);
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
  }*/
}
