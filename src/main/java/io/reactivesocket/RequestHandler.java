/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import io.reactivesocket.internal.PublisherUtils;

public abstract class RequestHandler {

	// TODO replace String with whatever ByteBuffer/byte[]/ByteBuf/etc variant we choose

	public abstract Publisher<Payload> handleRequestResponse(final Payload payload);

	public abstract Publisher<Payload> handleRequestStream(final Payload payload);

	public abstract Publisher<Payload> handleSubscription(final Payload payload);

	public abstract Publisher<Void> handleFireAndForget(final Payload payload);

	/**
	 * @param initialPayload
	 *            The first Payload contained in Publisher<Payload> 'payloads'.
	 *            <p>
	 *            This is delivered each time to allow simple use of the first payload for routing decisions.
	 * @param payloads
	 *            stream of Payloads.
	 * @return
	 */
	public abstract Publisher<Payload> handleChannel(final Payload initialPayload, final Publisher<Payload> payloads);

	public static RequestHandler create(
			Function<Payload, Publisher<Payload>> requestResponseHandler,
			Function<Payload, Publisher<Payload>> requestStreamHandler,
			Function<Payload, Publisher<Payload>> requestSubscriptionHandler,
			Function<Payload, Publisher<Void>> fireAndForgetHandler,
			BiFunction<Payload, Publisher<Payload>, Publisher<Payload>> channelHandler) {
		return new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(final Payload payload) {
				return requestResponseHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleRequestStream(final Payload payload) {
				return requestStreamHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleSubscription(final Payload payload) {
				return requestSubscriptionHandler.apply(payload);
			}

			@Override
			public Publisher<Void> handleFireAndForget(final Payload payload) {
				return fireAndForgetHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleChannel(final Payload initialPayload, final Publisher<Payload> payloads) {
				return channelHandler.apply(initialPayload, payloads);
			}

		};
	}

	public static RequestHandler create(
			Function<Payload, Publisher<Payload>> requestResponseHandler,
			Function<Payload, Publisher<Payload>> requestStreamHandler,
			Function<Payload, Publisher<Payload>> requestSubscriptionHandler,
			Function<Payload, Publisher<Void>> fireAndForgetHandler) {
		return new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(final Payload payload) {
				return requestResponseHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleRequestStream(final Payload payload) {
				return requestStreamHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleSubscription(final Payload payload) {
				return requestSubscriptionHandler.apply(payload);
			}

			@Override
			public Publisher<Void> handleFireAndForget(final Payload payload) {
				return fireAndForgetHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleChannel(final Payload initialPayload, final Publisher<Payload> payloads) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'channel' handler"));
			}

		};
	}

	public static RequestHandler create(
			Function<Payload, Publisher<Payload>> requestResponseHandler,
			Function<Payload, Publisher<Payload>> requestStreamHandler,
			Function<Payload, Publisher<Payload>> requestSubscriptionHandler) {
		return new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(final Payload payload) {
				return requestResponseHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleRequestStream(final Payload payload) {
				return requestStreamHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleSubscription(final Payload payload) {
				return requestSubscriptionHandler.apply(payload);
			}

			@Override
			public Publisher<Void> handleFireAndForget(final Payload payload) {
				return PublisherUtils.errorVoid(new RuntimeException("No 'fireAndForget' handler"));
			}

			@Override
			public Publisher<Payload> handleChannel(final Payload initialPayload, final Publisher<Payload> payloads) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'channel' handler"));
			}

		};
	}

	public static RequestHandler create(
			Function<Payload, Publisher<Payload>> requestResponseHandler,
			Function<Payload, Publisher<Payload>> requestStreamHandler) {
		return new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(final Payload payload) {
				return requestResponseHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleRequestStream(final Payload payload) {
				return requestStreamHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleSubscription(final Payload payload) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'requestSubscription' handler"));
			}

			@Override
			public Publisher<Void> handleFireAndForget(final Payload payload) {
				return PublisherUtils.errorVoid(new RuntimeException("No 'fireAndForget' handler"));
			}

			@Override
			public Publisher<Payload> handleChannel(final Payload initialPayload, final Publisher<Payload> payloads) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'channel' handler"));
			}

		};
	}

	public static RequestHandler create(
			Function<Payload, Publisher<Payload>> requestResponseHandler) {
		return new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(final Payload payload) {
				return requestResponseHandler.apply(payload);
			}

			@Override
			public Publisher<Payload> handleRequestStream(final Payload payload) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'requestStream' handler"));
			}

			@Override
			public Publisher<Payload> handleSubscription(final Payload payload) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'requestSubscription' handler"));
			}

			@Override
			public Publisher<Void> handleFireAndForget(final Payload payload) {
				return PublisherUtils.errorVoid(new RuntimeException("No 'fireAndForget' handler"));
			}

			@Override
			public Publisher<Payload> handleChannel(final Payload initialPayload, final Publisher<Payload> payloads) {
				return PublisherUtils.errorPayload(new RuntimeException("No 'channel' handler"));
			}

		};
	}
}
