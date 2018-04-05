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

package io.rsocket.framing;

import static io.rsocket.framing.FrameFactory.createFrame;
import static io.rsocket.framing.TestFrames.createTestCancelFrame;
import static io.rsocket.framing.TestFrames.createTestErrorFrame;
import static io.rsocket.framing.TestFrames.createTestExtensionFrame;
import static io.rsocket.framing.TestFrames.createTestKeepaliveFrame;
import static io.rsocket.framing.TestFrames.createTestLeaseFrame;
import static io.rsocket.framing.TestFrames.createTestMetadataPushFrame;
import static io.rsocket.framing.TestFrames.createTestPayloadFrame;
import static io.rsocket.framing.TestFrames.createTestRequestChannelFrame;
import static io.rsocket.framing.TestFrames.createTestRequestFireAndForgetFrame;
import static io.rsocket.framing.TestFrames.createTestRequestNFrame;
import static io.rsocket.framing.TestFrames.createTestRequestResponseFrame;
import static io.rsocket.framing.TestFrames.createTestRequestStreamFrame;
import static io.rsocket.framing.TestFrames.createTestResumeFrame;
import static io.rsocket.framing.TestFrames.createTestResumeOkFrame;
import static io.rsocket.framing.TestFrames.createTestSetupFrame;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class FrameFactoryTest {

  @DisplayName("creates CANCEL frame")
  @Test
  void createFrameCancel() {
    createTestCancelFrame()
        .consumeFrame(byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(CancelFrame.class));
  }

  @DisplayName("creates ERROR frame")
  @Test
  void createFrameError() {
    createTestErrorFrame()
        .consumeFrame(byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(ErrorFrame.class));
  }

  @DisplayName("creates EXT frame")
  @Test
  void createFrameExtension() {
    createTestExtensionFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(ExtensionFrame.class));
  }

  @DisplayName("creates KEEPALIVE frame")
  @Test
  void createFrameKeepalive() {
    createTestKeepaliveFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(KeepaliveFrame.class));
  }

  @DisplayName("creates METADATA_PUSH frame")
  @Test
  void createFrameMetadataPush() {
    createTestMetadataPushFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(MetadataPushFrame.class));
  }

  @DisplayName("creates PAYLOAD frame")
  @Test
  void createFramePayload() {
    createTestPayloadFrame()
        .consumeFrame(byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(PayloadFrame.class));
  }

  @DisplayName("creates REQUEST_CHANNEL frame")
  @Test
  void createFrameRequestChannel() {
    createTestRequestChannelFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(RequestChannelFrame.class));
  }

  @DisplayName("creates REQUEST_FNF frame")
  @Test
  void createFrameRequestFireAndForget() {
    createTestRequestFireAndForgetFrame()
        .consumeFrame(
            byteBuf ->
                assertThat(createFrame(byteBuf)).isInstanceOf(RequestFireAndForgetFrame.class));
  }

  @DisplayName("creates REQUEST_N frame")
  @Test
  void createFrameRequestN() {
    createTestRequestNFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(RequestNFrame.class));
  }

  @DisplayName("creates REQUEST_RESPONSE frame")
  @Test
  void createFrameRequestResponse() {
    createTestRequestResponseFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(RequestResponseFrame.class));
  }

  @DisplayName("creates REQUEST_STREAM frame")
  @Test
  void createFrameRequestStream() {
    createTestRequestStreamFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(RequestStreamFrame.class));
  }

  @DisplayName("creates RESUME frame")
  @Test
  void createFrameResume() {
    createTestResumeFrame()
        .consumeFrame(byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(ResumeFrame.class));
  }

  @DisplayName("creates RESUME_OK frame")
  @Test
  void createFrameResumeOk() {
    createTestResumeOkFrame()
        .consumeFrame(
            byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(ResumeOkFrame.class));
  }

  @DisplayName("creates SETUP frame")
  @Test
  void createFrameSetup() {
    createTestSetupFrame()
        .consumeFrame(byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(SetupFrame.class));
  }

  @DisplayName("creates LEASE frame")
  @Test
  void createLeaseSetup() {
    createTestLeaseFrame()
        .consumeFrame(byteBuf -> assertThat(createFrame(byteBuf)).isInstanceOf(LeaseFrame.class));
  }
}
