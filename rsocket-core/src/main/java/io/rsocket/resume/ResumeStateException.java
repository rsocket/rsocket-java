/*
 * Copyright 2015-2019 the original author or authors.
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

class ResumeStateException extends RuntimeException {
  private static final long serialVersionUID = -5393753463377588732L;
  private final long localPos;
  private final long localImpliedPos;
  private final long remotePos;
  private final long remoteImpliedPos;

  public ResumeStateException(
      long localPos, long localImpliedPos, long remotePos, long remoteImpliedPos) {
    this.localPos = localPos;
    this.localImpliedPos = localImpliedPos;
    this.remotePos = remotePos;
    this.remoteImpliedPos = remoteImpliedPos;
  }

  public long getLocalPos() {
    return localPos;
  }

  public long getLocalImpliedPos() {
    return localImpliedPos;
  }

  public long getRemotePos() {
    return remotePos;
  }

  public long getRemoteImpliedPos() {
    return remoteImpliedPos;
  }
}
