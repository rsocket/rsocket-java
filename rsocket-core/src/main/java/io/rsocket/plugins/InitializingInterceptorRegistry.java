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

import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import reactor.util.annotation.Nullable;

/**
 * Extends {@link InterceptorRegistry} with methods for building a chain of registered interceptors.
 * This is not intended for direct use by applications.
 */
public class InitializingInterceptorRegistry extends InterceptorRegistry {

  @Nullable
  public RequestInterceptor initRequesterRequestInterceptor(RSocket rSocketRequester) {
    return CompositeRequestInterceptor.create(
        rSocketRequester, getRequestInterceptorsForRequester());
  }

  @Nullable
  public RequestInterceptor initResponderRequestInterceptor(RSocket rSocketResponder) {
    return CompositeRequestInterceptor.create(
        rSocketResponder, getRequestInterceptorsForResponder());
  }

  public DuplexConnection initConnection(
      DuplexConnectionInterceptor.Type type, DuplexConnection connection) {
    for (DuplexConnectionInterceptor interceptor : getConnectionInterceptors()) {
      connection = interceptor.apply(type, connection);
    }
    return connection;
  }

  public RSocket initRequester(RSocket rsocket) {
    for (RSocketInterceptor interceptor : getRequesterInterceptors()) {
      rsocket = interceptor.apply(rsocket);
    }
    return rsocket;
  }

  public RSocket initResponder(RSocket rsocket) {
    for (RSocketInterceptor interceptor : getResponderInterceptors()) {
      rsocket = interceptor.apply(rsocket);
    }
    return rsocket;
  }

  public SocketAcceptor initSocketAcceptor(SocketAcceptor acceptor) {
    for (SocketAcceptorInterceptor interceptor : getSocketAcceptorInterceptors()) {
      acceptor = interceptor.apply(acceptor);
    }
    return acceptor;
  }

  public InitializingInterceptorRegistry copy() {
    final InitializingInterceptorRegistry registry = new InitializingInterceptorRegistry();

    registry.getRequestInterceptorsForRequester().addAll(this.getRequestInterceptorsForRequester());
    registry.getRequestInterceptorsForResponder().addAll(this.getRequestInterceptorsForResponder());
    registry.getConnectionInterceptors().addAll(this.getConnectionInterceptors());
    registry.getRequesterInterceptors().addAll(this.getRequesterInterceptors());
    registry.getResponderInterceptors().addAll(this.getResponderInterceptors());
    registry.getSocketAcceptorInterceptors().addAll(this.getSocketAcceptorInterceptors());

    return registry;
  }
}
