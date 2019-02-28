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

public class ResumeUtilTest {
  /*private Frame CANCEL = Frame.Cancel.from(1);
  private Frame STREAM =
      Frame.Request.from(1, FrameType.REQUEST_STREAM, DefaultPayload.create("Test"), 100);

  @Test
  public void testSupportedTypes() {
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_STREAM));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_CHANNEL));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_RESPONSE));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_N));
    assertTrue(ResumeUtil.isTracked(FrameType.CANCEL));
    assertTrue(ResumeUtil.isTracked(FrameType.ERROR));
    assertTrue(ResumeUtil.isTracked(FrameType.REQUEST_FNF));
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
  }*/
}
