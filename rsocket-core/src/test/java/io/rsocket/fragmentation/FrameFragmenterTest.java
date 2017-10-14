/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.fragmentation;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.util.PayloadImpl;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class FrameFragmenterTest {
  @Test
  public void testFragmentWithMetadataAndData() {
    ByteBuffer data = createRandomBytes(16);
    ByteBuffer metadata = createRandomBytes(16);

    Frame from =
        Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

    FrameFragmenter frameFragmenter = new FrameFragmenter(2);

    StepVerifier.create(frameFragmenter.fragment(from)).expectNextCount(16).verifyComplete();
  }

  @Test
  public void testFragmentWithMetadataAndDataWithOddData() {
    ByteBuffer data = createRandomBytes(17);
    ByteBuffer metadata = createRandomBytes(17);

    Frame from =
        Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

    FrameFragmenter frameFragmenter = new FrameFragmenter(2);

    StepVerifier.create(frameFragmenter.fragment(from)).expectNextCount(17).verifyComplete();
  }

  @Test
  public void testFragmentWithMetadataOnly() {
    ByteBuffer data = ByteBuffer.allocate(0);
    ByteBuffer metadata = createRandomBytes(16);

    Frame from =
        Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

    FrameFragmenter frameFragmenter = new FrameFragmenter(2);

    StepVerifier.create(frameFragmenter.fragment(from)).expectNextCount(8).verifyComplete();
  }

  @Test
  public void testFragmentWithDataOnly() {
    ByteBuffer data = createRandomBytes(16);
    ByteBuffer metadata = ByteBuffer.allocate(0);

    Frame from =
        Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

    FrameFragmenter frameFragmenter = new FrameFragmenter(2);

    StepVerifier.create(frameFragmenter.fragment(from)).expectNextCount(8).verifyComplete();
  }

  private ByteBuffer createRandomBytes(int size) {
    byte[] bytes = new byte[size];
    ThreadLocalRandom.current().nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }
}
