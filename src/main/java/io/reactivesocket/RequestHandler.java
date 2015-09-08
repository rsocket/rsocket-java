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

import io.reactivesocket.internal.PublisherUtils;
import org.reactivestreams.Publisher;

import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class RequestHandler {

	public static final Function<Payload, Publisher<Payload>> NO_REQUEST_RESPONSE_HANDLER =
		(payload -> PublisherUtils.errorPayload(new RuntimeException("No 'requestResponse' handler")));

	public static final Function<Payload, Publisher<Payload>> NO_REQUEST_STREAM_HANDLER =
		(payload -> PublisherUtils.errorPayload(new RuntimeException("No 'requestStream' handler")));

	public static final Function<Payload, Publisher<Payload>> NO_REQUEST_SUBSCRIPTION_HANDLER =
		(payload -> PublisherUtils.errorPayload(new RuntimeException("No 'requestSubscription' handler")));

	public static final Function<Payload, Publisher<Void>> NO_FIRE_AND_FORGET_HANDLER =
		(payload -> PublisherUtils.errorVoid(new RuntimeException("No 'fireAndForget' handler")));

	public static final BiFunction<Payload, Publisher<Payload>, Publisher<Payload>> NO_REQUEST_CHANNEL_HANDLER =
		(initialPayload, payloads) -> PublisherUtils.errorPayload(new RuntimeException("No 'requestChannel' handler"));

	public static final Function<Payload, Publisher<Void>> NO_METADATA_PUSH_HANDLER =
		(payload -> PublisherUtils.errorVoid(new RuntimeException("No 'metadataPush' handler")));

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

	public abstract Publisher<Void> handleMetadataPush(final Payload payload);

	public static class Builder
	{
		private Function<Payload, Publisher<Payload>> handleRequestResponse = NO_REQUEST_RESPONSE_HANDLER;
		private Function<Payload, Publisher<Payload>> handleRequestStream = NO_REQUEST_STREAM_HANDLER;
		private Function<Payload, Publisher<Payload>> handleRequestSubscription = NO_REQUEST_SUBSCRIPTION_HANDLER;
		private Function<Payload, Publisher<Void>> handleFireAndForget = NO_FIRE_AND_FORGET_HANDLER;
		private BiFunction<Payload, Publisher<Payload>, Publisher<Payload>> handleRequestChannel = NO_REQUEST_CHANNEL_HANDLER;
		private Function<Payload, Publisher<Void>> handleMetadataPush = NO_METADATA_PUSH_HANDLER;

		public Builder withRequestResponse(final Function<Payload, Publisher<Payload>> handleRequestResponse)
		{
			this.handleRequestResponse = handleRequestResponse;
			return this;
		}

		public Builder withRequestStream(final Function<Payload, Publisher<Payload>> handleRequestStream)
		{
			this.handleRequestStream = handleRequestStream;
			return this;
		}

		public Builder withRequestSubscription(final Function<Payload, Publisher<Payload>> handleRequestSubscription)
		{
			this.handleRequestSubscription = handleRequestSubscription;
			return this;
		}

		public Builder withFireAndForget(final Function<Payload, Publisher<Void>> handleFireAndForget)
		{
			this.handleFireAndForget = handleFireAndForget;
			return this;
		}

		public Builder withRequestChannel(final BiFunction<Payload, Publisher<Payload> , Publisher<Payload>> handleRequestChannel)
		{
			this.handleRequestChannel = handleRequestChannel;
			return this;
		}

		public Builder withMetadataPush(final Function<Payload, Publisher<Void>> handleMetadataPush)
		{
			this.handleMetadataPush = handleMetadataPush;
			return this;
		}

		public RequestHandler build()
		{
			return new RequestHandler()
			{
				public Publisher<Payload> handleRequestResponse(Payload payload)
				{
					return handleRequestResponse.apply(payload);
				}

				public Publisher<Payload> handleRequestStream(Payload payload)
				{
					return handleRequestStream.apply(payload);
				}

				public Publisher<Payload> handleSubscription(Payload payload)
				{
					return handleRequestSubscription.apply(payload);
				}

				public Publisher<Void> handleFireAndForget(Payload payload)
				{
					return handleFireAndForget.apply(payload);
				}

				public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads)
				{
					return handleRequestChannel.apply(initialPayload, payloads);
				}

				public Publisher<Void> handleMetadataPush(Payload payload)
				{
					return handleMetadataPush.apply(payload);
				}
			};
		}
	}
}
