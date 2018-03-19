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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class FrameTypeTest {

  @DisplayName("CANCEL characteristics")
  @Test
  void cancel() {
    assertThat(FrameType.CANCEL.canHaveData()).isFalse();
    assertThat(FrameType.CANCEL.canHaveMetadata()).isFalse();
    assertThat(FrameType.CANCEL.hasInitialRequestN()).isFalse();
    assertThat(FrameType.CANCEL.getEncodedType()).isEqualTo(0x09);
    assertThat(FrameType.CANCEL.isFragmentable()).isFalse();
    assertThat(FrameType.CANCEL.isRequestType()).isFalse();
  }

  @DisplayName("COMPLETE characteristics")
  @Test
  void complete() {
    assertThat(FrameType.COMPLETE.canHaveData()).isFalse();
    assertThat(FrameType.COMPLETE.canHaveMetadata()).isFalse();
    assertThat(FrameType.COMPLETE.hasInitialRequestN()).isFalse();
    assertThat(FrameType.COMPLETE.getEncodedType()).isEqualTo(0xB0);
    assertThat(FrameType.COMPLETE.isFragmentable()).isFalse();
    assertThat(FrameType.COMPLETE.isRequestType()).isFalse();
  }

  @DisplayName("ERROR characteristics")
  @Test
  void error() {
    assertThat(FrameType.ERROR.canHaveData()).isTrue();
    assertThat(FrameType.ERROR.canHaveMetadata()).isFalse();
    assertThat(FrameType.ERROR.getEncodedType()).isEqualTo(0x0B);
    assertThat(FrameType.ERROR.hasInitialRequestN()).isFalse();
    assertThat(FrameType.ERROR.isFragmentable()).isFalse();
    assertThat(FrameType.ERROR.isRequestType()).isFalse();
  }

  @DisplayName("EXT characteristics")
  @Test
  void ext() {
    assertThat(FrameType.EXT.canHaveData()).isTrue();
    assertThat(FrameType.EXT.canHaveMetadata()).isTrue();
    assertThat(FrameType.EXT.hasInitialRequestN()).isFalse();
    assertThat(FrameType.EXT.getEncodedType()).isEqualTo(0x3F);
    assertThat(FrameType.EXT.isFragmentable()).isFalse();
    assertThat(FrameType.EXT.isRequestType()).isFalse();
  }

  @DisplayName("KEEPALIVE characteristics")
  @Test
  void keepAlive() {
    assertThat(FrameType.KEEPALIVE.canHaveData()).isTrue();
    assertThat(FrameType.KEEPALIVE.canHaveMetadata()).isFalse();
    assertThat(FrameType.KEEPALIVE.getEncodedType()).isEqualTo(0x03);
    assertThat(FrameType.KEEPALIVE.hasInitialRequestN()).isFalse();
    assertThat(FrameType.KEEPALIVE.isFragmentable()).isFalse();
    assertThat(FrameType.KEEPALIVE.isRequestType()).isFalse();
  }

  @DisplayName("LEASE characteristics")
  @Test
  void lease() {
    assertThat(FrameType.LEASE.canHaveData()).isFalse();
    assertThat(FrameType.LEASE.canHaveMetadata()).isTrue();
    assertThat(FrameType.LEASE.getEncodedType()).isEqualTo(0x02);
    assertThat(FrameType.LEASE.hasInitialRequestN()).isFalse();
    assertThat(FrameType.LEASE.isFragmentable()).isFalse();
    assertThat(FrameType.LEASE.isRequestType()).isFalse();
  }

  @DisplayName("METADATA_PUSH characteristics")
  @Test
  void metadataPush() {
    assertThat(FrameType.METADATA_PUSH.canHaveData()).isFalse();
    assertThat(FrameType.METADATA_PUSH.canHaveMetadata()).isTrue();
    assertThat(FrameType.METADATA_PUSH.hasInitialRequestN()).isFalse();
    assertThat(FrameType.METADATA_PUSH.getEncodedType()).isEqualTo(0x0C);
    assertThat(FrameType.METADATA_PUSH.isFragmentable()).isFalse();
    assertThat(FrameType.METADATA_PUSH.isRequestType()).isFalse();
  }

  @DisplayName("NEXT characteristics")
  @Test
  void next() {
    assertThat(FrameType.NEXT.canHaveData()).isTrue();
    assertThat(FrameType.NEXT.canHaveMetadata()).isTrue();
    assertThat(FrameType.NEXT.hasInitialRequestN()).isFalse();
    assertThat(FrameType.NEXT.getEncodedType()).isEqualTo(0xA0);
    assertThat(FrameType.NEXT.isFragmentable()).isTrue();
    assertThat(FrameType.NEXT.isRequestType()).isFalse();
  }

  @DisplayName("NEXT_COMPLETE characteristics")
  @Test
  void nextComplete() {
    assertThat(FrameType.NEXT_COMPLETE.canHaveData()).isTrue();
    assertThat(FrameType.NEXT_COMPLETE.canHaveMetadata()).isTrue();
    assertThat(FrameType.NEXT_COMPLETE.hasInitialRequestN()).isFalse();
    assertThat(FrameType.NEXT_COMPLETE.getEncodedType()).isEqualTo(0xC0);
    assertThat(FrameType.NEXT_COMPLETE.isFragmentable()).isTrue();
    assertThat(FrameType.NEXT_COMPLETE.isRequestType()).isFalse();
  }

  @DisplayName("PAYLOAD characteristics")
  @Test
  void payload() {
    assertThat(FrameType.PAYLOAD.canHaveData()).isTrue();
    assertThat(FrameType.PAYLOAD.canHaveMetadata()).isTrue();
    assertThat(FrameType.PAYLOAD.hasInitialRequestN()).isFalse();
    assertThat(FrameType.PAYLOAD.getEncodedType()).isEqualTo(0x0A);
    assertThat(FrameType.PAYLOAD.isFragmentable()).isTrue();
    assertThat(FrameType.PAYLOAD.isRequestType()).isFalse();
  }

  @DisplayName("REQUEST_CHANNEL characteristics")
  @Test
  void requestChannel() {
    assertThat(FrameType.REQUEST_CHANNEL.canHaveData()).isTrue();
    assertThat(FrameType.REQUEST_CHANNEL.canHaveMetadata()).isTrue();
    assertThat(FrameType.REQUEST_CHANNEL.getEncodedType()).isEqualTo(0x07);
    assertThat(FrameType.REQUEST_CHANNEL.hasInitialRequestN()).isTrue();
    assertThat(FrameType.REQUEST_CHANNEL.isFragmentable()).isTrue();
    assertThat(FrameType.REQUEST_CHANNEL.isRequestType()).isTrue();
  }

  @DisplayName("REQUEST_FNF characteristics")
  @Test
  void requestFnf() {
    assertThat(FrameType.REQUEST_FNF.canHaveData()).isTrue();
    assertThat(FrameType.REQUEST_FNF.canHaveMetadata()).isTrue();
    assertThat(FrameType.REQUEST_FNF.getEncodedType()).isEqualTo(0x05);
    assertThat(FrameType.REQUEST_FNF.hasInitialRequestN()).isFalse();
    assertThat(FrameType.REQUEST_FNF.isFragmentable()).isTrue();
    assertThat(FrameType.REQUEST_FNF.isRequestType()).isTrue();
  }

  @DisplayName("REQUEST_N characteristics")
  @Test
  void requestN() {
    assertThat(FrameType.REQUEST_N.canHaveData()).isFalse();
    assertThat(FrameType.REQUEST_N.canHaveMetadata()).isFalse();
    assertThat(FrameType.REQUEST_N.getEncodedType()).isEqualTo(0x08);
    assertThat(FrameType.REQUEST_N.hasInitialRequestN()).isFalse();
    assertThat(FrameType.REQUEST_N.isFragmentable()).isFalse();
    assertThat(FrameType.REQUEST_N.isRequestType()).isFalse();
  }

  @DisplayName("REQUEST_RESPONSE characteristics")
  @Test
  void requestResponse() {
    assertThat(FrameType.REQUEST_RESPONSE.canHaveData()).isTrue();
    assertThat(FrameType.REQUEST_RESPONSE.canHaveMetadata()).isTrue();
    assertThat(FrameType.REQUEST_RESPONSE.getEncodedType()).isEqualTo(0x04);
    assertThat(FrameType.REQUEST_RESPONSE.hasInitialRequestN()).isFalse();
    assertThat(FrameType.REQUEST_RESPONSE.isFragmentable()).isTrue();
    assertThat(FrameType.REQUEST_RESPONSE.isRequestType()).isTrue();
  }

  @DisplayName("REQUEST_STREAM characteristics")
  @Test
  void requestStream() {
    assertThat(FrameType.REQUEST_STREAM.canHaveData()).isTrue();
    assertThat(FrameType.REQUEST_STREAM.canHaveMetadata()).isTrue();
    assertThat(FrameType.REQUEST_STREAM.getEncodedType()).isEqualTo(0x06);
    assertThat(FrameType.REQUEST_STREAM.hasInitialRequestN()).isTrue();
    assertThat(FrameType.REQUEST_STREAM.isFragmentable()).isTrue();
    assertThat(FrameType.REQUEST_STREAM.isRequestType()).isTrue();
  }

  @DisplayName("RESERVED characteristics")
  @Test
  void reserved() {
    assertThat(FrameType.RESERVED.canHaveData()).isFalse();
    assertThat(FrameType.RESERVED.canHaveMetadata()).isFalse();
    assertThat(FrameType.RESERVED.hasInitialRequestN()).isFalse();
    assertThat(FrameType.RESERVED.getEncodedType()).isEqualTo(0x00);
    assertThat(FrameType.RESERVED.isFragmentable()).isFalse();
    assertThat(FrameType.RESERVED.isRequestType()).isFalse();
  }

  @DisplayName("SETUP characteristics")
  @Test
  void setup() {
    assertThat(FrameType.SETUP.canHaveData()).isTrue();
    assertThat(FrameType.SETUP.canHaveMetadata()).isTrue();
    assertThat(FrameType.SETUP.getEncodedType()).isEqualTo(0x01);
    assertThat(FrameType.SETUP.hasInitialRequestN()).isFalse();
    assertThat(FrameType.SETUP.isFragmentable()).isFalse();
    assertThat(FrameType.SETUP.isRequestType()).isFalse();
  }
}
