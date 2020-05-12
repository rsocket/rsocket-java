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

import io.rsocket.RSocket;
import java.util.function.Function;

/**
 * Contract to decorate an {@link RSocket}, providing a way to intercept interactions. This can be
 * applied to a {@link InterceptorRegistry#forRequester(RSocketInterceptor) requester} or {@link
 * InterceptorRegistry#forResponder(RSocketInterceptor) responder} {@code RSocket} of a client or
 * server.
 */
public @FunctionalInterface interface RSocketInterceptor extends Function<RSocket, RSocket> {}
