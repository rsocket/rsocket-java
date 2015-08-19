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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.reactivesocket.internal.Requester;
import io.reactivesocket.internal.Responder;
import io.reactivesocket.internal.UnicastSubject;
import rx.Observable;

import org.reactivestreams.Subscriber;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import static rx.Observable.error;
import static rx.RxReactiveStreams.toPublisher;
import static rx.RxReactiveStreams.toObservable;

/**
 * Interface for a connection that supports sending requests and receiving responses
 *
 * Created by servers for connections
 * Created on demand for clients
 */
public class ReactiveSocket
{
    private static final Publisher<Payload> NOT_FOUND_ERROR_PAYLOAD = toPublisher(error(new Exception("Not Found!")));
    private static final Publisher<Void> NOT_FOUND_ERROR_VOID = toPublisher(error(new Exception("Not Found!")));
    private static final RequestHandler EMPTY_HANDLER = new RequestHandler()
    {
        public Publisher<Payload> handleRequestResponse(Payload payload)
        {
            return NOT_FOUND_ERROR_PAYLOAD;
        }

        public Publisher<Payload> handleRequestStream(Payload payload)
        {
            return NOT_FOUND_ERROR_PAYLOAD;
        }

        public Publisher<Payload> handleRequestSubscription(Payload payload)
        {
            return NOT_FOUND_ERROR_PAYLOAD;
        }

        public Publisher<Void> handleFireAndForget(Payload payload)
        {
            return NOT_FOUND_ERROR_VOID;
        }
    };
    private static final Responder EMPTY_RESPONDER = Responder.create(EMPTY_HANDLER);

    private final boolean isServer;
    private final Requester requester;

    private final Long2ObjectHashMap<UnicastSubject> requesterStreamInputMap = new Long2ObjectHashMap<>();

    public static ReactiveSocket createRequestor(final DuplexConnection connection)
    {
        return connect(connection, EMPTY_HANDLER);
    }

    // TODO private for now as the bi-directional bit is NOT working
    
    /**
     * Create a ReactiveSocket and initiate connect processing as a client
     *
     * @param connection
     * @param requestHandler
     * @return
     */
    private static ReactiveSocket connect(final DuplexConnection connection, final RequestHandler requestHandler)
    {
        final ReactiveSocket socket = new ReactiveSocket(false, connection, requestHandler);

        // TODO: initiate connect logic as a client

        return socket;
    }

    // TODO what name makes sense for these 'create' methods?
    // TODO what if someone wants to create just a Responder? This class has the 'request' methods on it.
    
    /**
     * Create a ReactiveSocket and prepare for operation as a server
     *
     * @param connection
     * @param requestHandler
     * @return
     */
    public static ReactiveSocket createResponderAndRequestor(final DuplexConnection connection, final RequestHandler requestHandler)
    {
        final ReactiveSocket socket = new ReactiveSocket(true, connection, requestHandler);

        // TODO: passively wait for a SETUP and accept or reject it

        return socket;
    }

    /**
     * Initiate a request response exchange
     *
     * @param data
     * @param metadata
     * @return
     */
    public Publisher<Payload> requestResponse(final Payload payload)
    {
        return requester.requestResponse(payload);
    }

    public Publisher<Void> fireAndForget(final Payload payload)
    {
        return requester.fireAndForget(payload);
    }

    public Publisher<Payload> requestStream(final Payload payload)
    {
        return requester.requestStream(payload);
    }

    public Publisher<Payload> requestSubscription(final Payload payload)
    {
        return requester.requestSubscription(payload);
    }

    private ReactiveSocket(final boolean isServer, final DuplexConnection connection, final RequestHandler requestHandler)
    {
		this.isServer = isServer;
		// connect the Requestor
		Publisher<Void> requestorConnectionHandler = toPublisher(Observable.empty()); // temporary until fix birectional
		// connect the Responder
        Publisher<Void> responderConnectionHandler = toPublisher(Observable.empty()); // temporary until fix birectional
        
        
		if (isServer) {
			this.requester = null; // TODO until we fix the bidirectional issue
	        
	        if(requestHandler == EMPTY_HANDLER) {
	        	responderConnectionHandler = EMPTY_RESPONDER.acceptConnection(connection);
	        } else {
	        	responderConnectionHandler = Responder.create(requestHandler).acceptConnection(connection);
	        }
	        
		} else {
			this.requester = Requester.createForConnection(isServer, connection);
			requestorConnectionHandler = this.requester.start();
		}

        // merge Publishers and subscribe to start processing messages and handle errors
		Observable.merge(toObservable(requestorConnectionHandler), toObservable(responderConnectionHandler))
		.doOnSubscribe(() -> System.out.println("... Starting ReactiveSocket " + (isServer ? "Server" : "Client")))
		.doOnUnsubscribe(() -> System.out.println("... Shutting Down ReactiveSocket " + (isServer ? "Server" : "Client")))
				.subscribe(new rx.Subscriber<Void>() {
					public void onNext(Void frame) {
					}

					public void onError(Throwable t) {
						requesterStreamInputMap.forEach((id, subject) -> subject.onError(t));
						// TODO: iterate over responder side and destroy world
						System.out.println("-- onError ReactiveSocket " + (isServer ? "Server" : "Client"));
						t.printStackTrace();
					}

					public void onCompleted() {
						// TODO: might be a RuntimeException
						requesterStreamInputMap.forEach((id, subject) -> subject.onCompleted());
						// TODO: iterate over responder side and destroy world
						System.out.println("-- onComplete ReactiveSocket " + (isServer ? "Server" : "Client"));
					}
				});
        
        // TODO need to unsubscribe / clean up this connection somehow

    }
}
