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

package io.rsocket.resume;

import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.util.retry.Retry;

/**
 * @deprecated as of 1.0 RC7 in favor of using {@link io.rsocket.core.Resume#retry(Retry)} via
 *     {@link io.rsocket.core.RSocketConnector} or {@link io.rsocket.core.RSocketServer}.
 */
@Deprecated
@FunctionalInterface
public interface ResumeStrategy extends BiFunction<ClientResume, Throwable, Publisher<?>> {}
