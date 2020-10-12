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
import java.util.function.Supplier;

/**
 * Provides support for registering interceptors at the following levels:
 *
 * <ul>
 *   <li>{@link #forConnection(DuplexConnectionInterceptor)} -- transport level
 *   <li>{@link #forSocketAcceptor(SocketAcceptorInterceptor)} -- for accepting new connections
 *   <li>{@link #forRequester(RSocketInterceptor)} -- for performing of requests
 *   <li>{@link #forResponder(RSocketInterceptor)} -- for responding to requests
 * </ul>
 */
public class InterceptorRegistry {
  private List<Supplier<? extends RequestInterceptor>> requesterRequestInterceptors =
      new ArrayList<>();
  private List<Supplier<? extends RequestInterceptor>> responderRequestInterceptors =
      new ArrayList<>();
  private List<RSocketInterceptor> requesterRSocketInterceptors = new ArrayList<>();
  private List<RSocketInterceptor> responderRSocketInterceptors = new ArrayList<>();
  private List<SocketAcceptorInterceptor> socketAcceptorInterceptors = new ArrayList<>();
  private List<DuplexConnectionInterceptor> connectionInterceptors = new ArrayList<>();

  /** Add an {@link RequestInterceptor} that will hook into Requester RSocket requests' phases. */
  public InterceptorRegistry forRequesterRequests(
      Supplier<? extends RequestInterceptor> interceptor) {
    requesterRequestInterceptors.add(interceptor);
    return this;
  }

  /**
   * Variant of {@link #forRequesterRequests(Supplier)} with access to the list of existing
   * registrations.
   */
  public InterceptorRegistry forRequesterRequests(
      Consumer<List<Supplier<? extends RequestInterceptor>>> consumer) {
    consumer.accept(requesterRequestInterceptors);
    return this;
  }

  /** Add an {@link RequestInterceptor} that will hook into Requester RSocket requests' phases. */
  public InterceptorRegistry forResponderRequests(
      Supplier<? extends RequestInterceptor> interceptor) {
    responderRequestInterceptors.add(interceptor);
    return this;
  }

  /**
   * Variant of {@link #forResponderRequests(Supplier)} with access to the list of existing
   * registrations.
   */
  public InterceptorRegistry forResponderRequests(
      Consumer<List<Supplier<? extends RequestInterceptor>>> consumer) {
    consumer.accept(responderRequestInterceptors);
    return this;
  }

  /**
   * Add an {@link RSocketInterceptor} that will decorate the RSocket used for performing requests.
   */
  public InterceptorRegistry forRequester(RSocketInterceptor interceptor) {
    requesterRSocketInterceptors.add(interceptor);
    return this;
  }

  /**
   * Variant of {@link #forRequester(RSocketInterceptor)} with access to the list of existing
   * registrations.
   */
  public InterceptorRegistry forRequester(Consumer<List<RSocketInterceptor>> consumer) {
    consumer.accept(requesterRSocketInterceptors);
    return this;
  }

  /**
   * Add an {@link RSocketInterceptor} that will decorate the RSocket used for resonding to
   * requests.
   */
  public InterceptorRegistry forResponder(RSocketInterceptor interceptor) {
    responderRSocketInterceptors.add(interceptor);
    return this;
  }

  /**
   * Variant of {@link #forResponder(RSocketInterceptor)} with access to the list of existing
   * registrations.
   */
  public InterceptorRegistry forResponder(Consumer<List<RSocketInterceptor>> consumer) {
    consumer.accept(responderRSocketInterceptors);
    return this;
  }

  /**
   * Add a {@link SocketAcceptorInterceptor} that will intercept the accepting of new connections.
   */
  public InterceptorRegistry forSocketAcceptor(SocketAcceptorInterceptor interceptor) {
    socketAcceptorInterceptors.add(interceptor);
    return this;
  }

  /**
   * Variant of {@link #forSocketAcceptor(SocketAcceptorInterceptor)} with access to the list of
   * existing registrations.
   */
  public InterceptorRegistry forSocketAcceptor(Consumer<List<SocketAcceptorInterceptor>> consumer) {
    consumer.accept(socketAcceptorInterceptors);
    return this;
  }

  /** Add a {@link DuplexConnectionInterceptor}. */
  public InterceptorRegistry forConnection(DuplexConnectionInterceptor interceptor) {
    connectionInterceptors.add(interceptor);
    return this;
  }

  /**
   * Variant of {@link #forConnection(DuplexConnectionInterceptor)} with access to the list of
   * existing registrations.
   */
  public InterceptorRegistry forConnection(Consumer<List<DuplexConnectionInterceptor>> consumer) {
    consumer.accept(connectionInterceptors);
    return this;
  }

  List<Supplier<? extends RequestInterceptor>> getRequesterRequestInterceptors() {
    return requesterRequestInterceptors;
  }

  List<Supplier<? extends RequestInterceptor>> getResponderRequestInterceptors() {
    return responderRequestInterceptors;
  }

  List<RSocketInterceptor> getRequesterInterceptors() {
    return requesterRSocketInterceptors;
  }

  List<RSocketInterceptor> getResponderInterceptors() {
    return responderRSocketInterceptors;
  }

  List<DuplexConnectionInterceptor> getConnectionInterceptors() {
    return connectionInterceptors;
  }

  List<SocketAcceptorInterceptor> getSocketAcceptorInterceptors() {
    return socketAcceptorInterceptors;
  }
}
