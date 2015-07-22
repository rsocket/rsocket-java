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

import static rx.Observable.*;
import static rx.RxReactiveStreams.*;

import java.util.concurrent.ConcurrentHashMap;

import org.reactivestreams.Publisher;

import rx.functions.Func1;

public class ReactiveSocketServerProtocol {

	private Func1<String, Publisher<String>> requestResponseHandler;
	private Func1<String, Publisher<String>> requestStreamHandler;

	private ReactiveSocketServerProtocol(
			Func1<String, Publisher<String>> requestResponseHandler,
			Func1<String, Publisher<String>> requestStreamHandler) {
		this.requestResponseHandler = requestResponseHandler;
		this.requestStreamHandler = requestStreamHandler;
	}

	public static ReactiveSocketServerProtocol create(
			Func1<String, Publisher<String>> requestResponseHandler,
			Func1<String, Publisher<String>> requestStreamHandler) {
		return new ReactiveSocketServerProtocol(requestResponseHandler, requestStreamHandler);
	}

	public Publisher<Void> acceptConnection(DuplexConnection ws) {
		/* state of cancellation subjects during connection */
		// TODO consider using the LongObjectHashMap from Agrona for perf improvement
		// TODO consider alternate to PublishSubject that assumes a single subscriber and is lighter
		final ConcurrentHashMap<Integer, CancellationToken> cancellationObservables = new ConcurrentHashMap<>();

		return toPublisher(toObservable(ws.getInput()).flatMap(message -> {
			if (message.getMessageType() == MessageType.SUBSCRIBE_REQUEST_RESPONSE) {
				CancellationToken cancellationToken = CancellationToken.create();
				cancellationObservables.put(message.getMessageId(), cancellationToken);

				return toObservable(requestResponseHandler.call(message.getMessage()))
						.single() // enforce that it is a request/response
						.map(v -> Message.from(message.getMessageId(), MessageType.NEXT_COMPLETE, v))
						.onErrorReturn(err -> Message.from(message.getMessageId(), MessageType.ERROR, err.getMessage()))
						.flatMap(payload -> toObservable(ws.write(payload)))
						.takeUntil(cancellationToken)
						.finallyDo(() -> cancellationObservables.remove(message.getMessageId()));
			} else if (message.getMessageType() == MessageType.SUBSCRIBE_STREAM) {
				CancellationToken cancellationToken = CancellationToken.create();
				cancellationObservables.put(message.getMessageId(), cancellationToken);

				//@formatter:off
				return toObservable(requestStreamHandler.call(message.getMessage()))
						.flatMap(s ->   toObservable(ws.write(Message.from(message.getMessageId(), MessageType.NEXT, s))), 
								 err -> toObservable(ws.write(Message.from(message.getMessageId(), MessageType.ERROR, err.getMessage()))), 
								 () ->  toObservable(ws.write(Message.from(message.getMessageId(), MessageType.COMPLETE, "")))
						)
						.takeUntil(cancellationToken)
						.finallyDo(() -> cancellationObservables.remove(message.getMessageId()));
				//@formatter:on
			} else if (message.getMessageType() == MessageType.DISPOSE) {
				CancellationToken cancellationToken = cancellationObservables.get(message.getMessageId());
				if (cancellationToken != null) {
					cancellationToken.cancel();
				}
				return empty();
			} else {
				return error(new IllegalStateException("Unexpected prefix: " + message.getMessageType()));
			}
		}));
	}

}
