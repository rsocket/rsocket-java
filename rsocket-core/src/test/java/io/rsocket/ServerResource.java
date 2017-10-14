/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

public class ServerResource {
  private Function<Payload, Mono<Payload>> acceptor;

  private final Subscriber<Void> connectSub;
  private final RSocketServer socket;
  private final RSocket acceptingSocket;

  public final TestDuplexConnection connection;
  public final ConcurrentLinkedQueue<Throwable> errors;

  public ServerResource() {
    acceptor = Mono::just;
    connection = new TestDuplexConnection();
    connectSub = TestSubscriber.create();
    errors = new ConcurrentLinkedQueue<>();
    acceptingSocket =
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return acceptor.apply(payload);
          }
        };
    socket = new RSocketServer(connection, acceptingSocket, errors::add);
  }

  public void sendRequest(int streamId, FrameType frameType) {
    Frame request = Frame.Request.from(streamId, frameType, PayloadImpl.EMPTY, 1);
    connection.addToReceivedBuffer(request);
    connection.addToReceivedBuffer(Frame.RequestN.from(streamId, 2));
  }

  public void acceptor(Function<Payload, Mono<Payload>> acceptor) {
    this.acceptor = acceptor;
  }
}
