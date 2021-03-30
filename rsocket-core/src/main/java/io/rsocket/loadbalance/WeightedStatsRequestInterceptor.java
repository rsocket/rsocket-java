/*
 * Copyright 2015-2021 the original author or authors.
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
package io.rsocket.loadbalance;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import io.rsocket.plugins.RequestInterceptor;
import reactor.util.annotation.Nullable;

/**
 * {@link RequestInterceptor} that hooks into request lifecycle and calls methods of the parent
 * class to manage tracking state and expose {@link WeightedStats}.
 *
 * <p>This interceptor the default mechanism for gathering stats when {@link
 * WeightedLoadbalanceStrategy} is used with {@link LoadbalanceRSocketClient}.
 *
 * @since 1.1
 * @see LoadbalanceRSocketClient
 * @see WeightedLoadbalanceStrategy
 */
public class WeightedStatsRequestInterceptor extends BaseWeightedStats
    implements RequestInterceptor {

  final Int2LongHashMap requestsStartTime = new Int2LongHashMap(-1);

  public WeightedStatsRequestInterceptor() {
    super();
  }

  @Override
  public final void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
    switch (requestType) {
      case REQUEST_FNF:
      case REQUEST_RESPONSE:
        final long startTime = startRequest();
        final Int2LongHashMap requestsStartTime = this.requestsStartTime;
        synchronized (requestsStartTime) {
          requestsStartTime.put(streamId, startTime);
        }
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        this.startStream();
    }
  }

  @Override
  public final void onTerminate(int streamId, FrameType requestType, @Nullable Throwable t) {
    switch (requestType) {
      case REQUEST_FNF:
      case REQUEST_RESPONSE:
        long startTime;
        final Int2LongHashMap requestsStartTime = this.requestsStartTime;
        synchronized (requestsStartTime) {
          startTime = requestsStartTime.remove(streamId);
        }
        long endTime = stopRequest(startTime);
        if (t == null) {
          record(endTime - startTime);
        }
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        stopStream();
        break;
    }

    if (t != null) {
      updateAvailability(0.0d);
    } else {
      updateAvailability(1.0d);
    }
  }

  @Override
  public final void onCancel(int streamId, FrameType requestType) {
    switch (requestType) {
      case REQUEST_FNF:
      case REQUEST_RESPONSE:
        long startTime;
        final Int2LongHashMap requestsStartTime = this.requestsStartTime;
        synchronized (requestsStartTime) {
          startTime = requestsStartTime.remove(streamId);
        }
        stopRequest(startTime);
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        stopStream();
        break;
    }
  }

  @Override
  public final void onReject(Throwable rejectionReason, FrameType requestType, ByteBuf metadata) {}

  @Override
  public void dispose() {}
}
