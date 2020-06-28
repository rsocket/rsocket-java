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

package io.rsocket.transport;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.rsocket.DuplexConnection;

/** */
public interface Transport {

  /**
   * Configurations that exposes the maximum frame size that a {@link DuplexConnection} can bring up
   * to RSocket level.
   *
   * <p>This number should not exist the 16,777,215 (maximum frame size specified by RSocket spec)
   *
   * @return return maximum configured frame size limit
   */
  default int maxFrameLength() {
    return FRAME_LENGTH_MASK;
  }
}
