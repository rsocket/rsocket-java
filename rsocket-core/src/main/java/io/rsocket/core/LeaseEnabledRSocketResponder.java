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

package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.lease.RequestTracker;
import io.rsocket.lease.ResponderLeaseHandler;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;

/** Lease aware implementation of the responder side of RSocket */
final class LeaseEnabledRSocketResponder extends RSocketResponder {

  private final ResponderLeaseHandler leaseHandler;
  private final Disposable leaseHandlerDisposable;
  @Nullable private final RequestTracker requestTracker;

  LeaseEnabledRSocketResponder(
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      ResponderLeaseHandler leaseHandler,
      @Nullable RequestTracker requestTracker,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize) {
    super(connection, requestHandler, payloadDecoder, mtu, maxFrameLength, maxInboundPayloadSize);

    this.leaseHandler = leaseHandler;
    this.requestTracker = requestTracker;
    this.leaseHandlerDisposable =
        leaseHandler.send(this.getSendProcessor()::onNextPrioritized, requestTracker);
  }

  @Override
  void doOnDispose(Throwable e) {
    this.leaseHandlerDisposable.dispose();
    final RequestTracker requestTracker = this.requestTracker;
    if (requestTracker != null) {
      requestTracker.dispose();
    }

    super.doOnDispose(e);
  }

  @Override
  void handleFireAndForget(int streamId, ByteBuf frame) {
    if (leaseHandler.useLease()) {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onStart(
            streamId, FrameType.REQUEST_FNF, RequestFireAndForgetFrameCodec.metadata(frame));
        if (FrameHeaderCodec.hasFollows(frame)) {
          FireAndForgetResponderSubscriber subscriber =
              new FireAndForgetResponderSubscriber(streamId, frame, this, this);

          this.add(streamId, subscriber);
        } else {
          fireAndForget(super.getPayloadDecoder().apply(frame))
              .subscribe(new FireAndForgetResponderSubscriber(streamId, this));
        }
      } else {
        super.handleFireAndForget(streamId, frame);
      }
    } else {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onReject(
            streamId, FrameType.REQUEST_FNF, RequestFireAndForgetFrameCodec.metadata(frame));
      }
    }
  }

  @Override
  void handleRequestResponse(int streamId, ByteBuf frame) {
    if (leaseHandler.useLease()) {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onStart(
            streamId, FrameType.REQUEST_RESPONSE, RequestResponseFrameCodec.metadata(frame));
      }
      super.handleRequestResponse(streamId, frame);
    } else {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onReject(
            streamId, FrameType.REQUEST_RESPONSE, RequestResponseFrameCodec.metadata(frame));
      }
      sendLeaseRejection(streamId);
    }
  }

  @Override
  void handleStream(int streamId, ByteBuf frame, long initialRequestN) {
    if (leaseHandler.useLease()) {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onStart(
            streamId, FrameType.REQUEST_STREAM, RequestStreamFrameCodec.metadata(frame));
      }
      super.handleStream(streamId, frame, initialRequestN);
    } else {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onReject(
            streamId, FrameType.REQUEST_STREAM, RequestStreamFrameCodec.metadata(frame));
      }
      sendLeaseRejection(streamId);
    }
  }

  @Override
  void handleChannel(int streamId, ByteBuf frame, long initialRequestN, boolean complete) {
    if (leaseHandler.useLease()) {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onStart(
            streamId, FrameType.REQUEST_CHANNEL, RequestChannelFrameCodec.metadata(frame));
      }
      super.handleChannel(streamId, frame, initialRequestN, complete);
    } else {
      final RequestTracker requestTracker = this.requestTracker;
      if (requestTracker != null) {
        requestTracker.onReject(
            streamId, FrameType.REQUEST_CHANNEL, RequestChannelFrameCodec.metadata(frame));
      }
      sendLeaseRejection(streamId);
    }
  }

  void sendLeaseRejection(int streamId) {
    getSendProcessor()
        .onNext(ErrorFrameCodec.encode(getAllocator(), streamId, leaseHandler.leaseError()));
  }

  @Override
  public RequestTracker getRequestTracker() {
    return requestTracker;
  }
}
