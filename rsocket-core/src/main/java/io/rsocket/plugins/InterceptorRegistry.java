/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket.plugins;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class InterceptorRegistry {
  private List<DuplexConnectionInterceptor> connectionInterceptors = new ArrayList<>();
  private List<RSocketInterceptor> requesterInteceptors = new ArrayList<>();
  private List<RSocketInterceptor> responderInterceptors = new ArrayList<>();
  private List<SocketAcceptorInterceptor> socketAcceptorInterceptors = new ArrayList<>();

  public InterceptorRegistry forConnection(DuplexConnectionInterceptor interceptor) {
    connectionInterceptors.add(interceptor);
    return this;
  }

  public InterceptorRegistry forConnection(Consumer<List<DuplexConnectionInterceptor>> consumer) {
    consumer.accept(connectionInterceptors);
    return this;
  }

  public InterceptorRegistry forRequester(RSocketInterceptor interceptor) {
    requesterInteceptors.add(interceptor);
    return this;
  }

  public InterceptorRegistry forRequester(Consumer<List<RSocketInterceptor>> consumer) {
    consumer.accept(requesterInteceptors);
    return this;
  }

  public InterceptorRegistry forResponder(RSocketInterceptor interceptor) {
    responderInterceptors.add(interceptor);
    return this;
  }

  public InterceptorRegistry forResponder(Consumer<List<RSocketInterceptor>> consumer) {
    consumer.accept(responderInterceptors);
    return this;
  }

  public InterceptorRegistry forSocketAcceptor(SocketAcceptorInterceptor interceptor) {
    socketAcceptorInterceptors.add(interceptor);
    return this;
  }

  public InterceptorRegistry forSocketAcceptor(Consumer<List<SocketAcceptorInterceptor>> consumer) {
    consumer.accept(socketAcceptorInterceptors);
    return this;
  }

  List<DuplexConnectionInterceptor> getConnectionInterceptors() {
    return connectionInterceptors;
  }

  List<RSocketInterceptor> getRequesterInteceptors() {
    return requesterInteceptors;
  }

  List<RSocketInterceptor> getResponderInterceptors() {
    return responderInterceptors;
  }

  List<SocketAcceptorInterceptor> getSocketAcceptorInterceptors() {
    return socketAcceptorInterceptors;
  }
}
