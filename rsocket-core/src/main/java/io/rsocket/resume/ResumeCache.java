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

import io.netty.buffer.ByteBuf;
import java.util.*;
import reactor.core.publisher.Flux;

public class ResumeCache {
  private final ResumePositionCounter strategy;
  private final int maxBufferSize;

  private final LinkedHashMap<Integer, ByteBuf> frames = new LinkedHashMap<>();
  private int lastRemotePosition = 0;
  private int currentPosition = 0;
  private int bufferSize;

  public ResumeCache(ResumePositionCounter strategy, int maxBufferSize) {
    this.strategy = strategy;
    this.maxBufferSize = maxBufferSize;
  }

  public void updateRemotePosition(int remotePosition) {
    if (remotePosition > currentPosition) {
      throw new IllegalStateException(
          "Remote ahead of " + lastRemotePosition + " , expected " + remotePosition);
    }

    if (remotePosition == lastRemotePosition) {
      return;
    }

    if (remotePosition < lastRemotePosition) {
      throw new IllegalStateException(
          "Remote position moved back from " + lastRemotePosition + " to " + remotePosition);
    }

    lastRemotePosition = remotePosition;

    Iterator<Map.Entry<Integer, ByteBuf>> positions = frames.entrySet().iterator();

    while (positions.hasNext()) {
      Map.Entry<Integer, ByteBuf> cachePosition = positions.next();

      if (cachePosition.getKey() <= remotePosition) {
        positions.remove();
        bufferSize -= strategy.cost(cachePosition.getValue());
        cachePosition.getValue().release();
      }

      // TODO check for a bad position
    }
  }

  public void sent(ByteBuf frame) {
    if (ResumeUtil.isTracked(frame)) {
      frames.put(currentPosition, frame.copy());
      bufferSize += strategy.cost(frame);

      currentPosition += ResumeUtil.offset(frame);

      if (frames.size() > maxBufferSize) {
        ByteBuf f = frames.remove(first(frames));
        bufferSize -= strategy.cost(f);
      }
    }
  }

  private int first(LinkedHashMap<Integer, ByteBuf> frames) {
    return frames.keySet().iterator().next();
  }

  public Flux<ByteBuf> resend(int remotePosition) {
    updateRemotePosition(remotePosition);

    if (remotePosition == currentPosition) {
      return Flux.empty();
    }

    List<ByteBuf> resend = new ArrayList<>();

    for (Map.Entry<Integer, ByteBuf> cachePosition : frames.entrySet()) {
      if (remotePosition < cachePosition.getKey()) {
        resend.add(cachePosition.getValue());
      }

      // TODO error handling
    }

    return Flux.fromIterable(resend);
  }

  public int getCurrentPosition() {
    return currentPosition;
  }

  public int getRemotePosition() {
    return lastRemotePosition;
  }

  public int getEarliestResendPosition() {
    if (frames.isEmpty()) {
      return currentPosition;
    } else {
      return first(frames);
    }
  }

  public int size() {
    return bufferSize;
  }
}
