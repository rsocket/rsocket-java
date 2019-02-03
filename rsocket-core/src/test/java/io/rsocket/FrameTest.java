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

package io.rsocket;

public class FrameTest {
  /*@Test
  public void testFrameToString() {
    final io.rsocket.Frame requestFrame =
        io.rsocket.Frame.Request.from(
            1, FrameType.REQUEST_RESPONSE, DefaultPayload.create("streaming in -> 0"), 1);
    assertEquals(
        "Frame => Stream ID: 1 Type: REQUEST_RESPONSE Payload: data: \"streaming in -> 0\" ",
        requestFrame.toString());
  }

  @Test
  public void testFrameWithMetadataToString() {
    final io.rsocket.Frame requestFrame =
        io.rsocket.Frame.Request.from(
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
    io.rsocket.Frame frame =
        io.rsocket.Frame.PayloadFrame.from(
            1,
            FrameType.NEXT_COMPLETE,
            DefaultPayload.create("Hello"),
            FrameHeaderFlyweight.FLAGS_C);
    frame.toString();
  }*/
}
