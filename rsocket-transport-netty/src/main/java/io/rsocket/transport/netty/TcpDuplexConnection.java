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

package io.rsocket.transport.netty;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyInbound;
import reactor.ipc.netty.NettyOutbound;

/** An implementation of {@link DuplexConnection} that connects via TCP. */
public final class TcpDuplexConnection implements DuplexConnection {

  private final NettyContext context;

  private final NettyInbound in;

  private final NettyOutbound out;

  /**
   * Creates a new instance
   *
   * @param in the {@link NettyInbound} to listen on
   * @param out the {@link NettyOutbound} to send with
   * @param context the {@link NettyContext} to for managing the server
   */
  public TcpDuplexConnection(NettyInbound in, NettyOutbound out, NettyContext context) {
    this.in = in;
    this.out = out;
    this.context = context;
  }

  @Override
  public void dispose() {
    context.dispose();
  }

  @Override
  public boolean isDisposed() {
    return context.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return context.onClose();
  }

  @Override
  public Flux<Frame> receive() {
    return in.receive().map(buf -> Frame.from(buf.retain()));
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames).concatMap(this::sendOne).then();
  }

  @Override
  public Mono<Void> sendOne(Frame frame) {
    return out.sendObject(frame.content()).then();
  }
}
