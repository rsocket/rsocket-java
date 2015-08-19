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

import org.reactivestreams.Publisher;

import io.reactivesocket.internal.Requester;
import io.reactivesocket.internal.Responder;
import io.reactivesocket.internal.UnicastSubject;
import rx.Observable;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

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
    private Requester requester; // can't initialized until connection is accepted
    private final Responder responder;

    public static ReactiveSocket createRequestor()
    {
        return new ReactiveSocket(false, EMPTY_HANDLER);
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
    public static ReactiveSocket createResponderAndRequestor(final RequestHandler requestHandler)
    {
        final ReactiveSocket socket = new ReactiveSocket(true, requestHandler);

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
    	assertRequester();
        return requester.requestResponse(payload);
    }

    public Publisher<Void> fireAndForget(final Payload payload)
    {
    	assertRequester();
        return requester.fireAndForget(payload);
    }

    public Publisher<Payload> requestStream(final Payload payload)
    {
    	assertRequester();
        return requester.requestStream(payload);
    }

    public Publisher<Payload> requestSubscription(final Payload payload)
    {
    	assertRequester();
        return requester.requestSubscription(payload);
    }

	private void assertRequester() {
		if(requester == null) {
    		throw new IllegalStateException("Connection not initialized. Please 'acceptConnection' before submitting requests");
    	}
	}

    private ReactiveSocket(final boolean isServer, final RequestHandler requestHandler)
    {
		this.isServer = isServer;
		this.responder = Responder.create(requestHandler);


	}
    
    /**
     * Connect this ReactiveSocket with the given DuplexConnection.
     * <p>
     * NOTE: You must subscribe to the returned Publisher for anything to start.
     * 
     * @param connection
     * @return
     */
	public Publisher<Void> connect(DuplexConnection connection) {
		// TODO should we make this eager instead of lazy so people don't have to subscribe to the publisher if they want to ignore errors? how should errors then be handled?
		
		// connect the Requestor
		Publisher<Void> requesterConnectionHandler = toPublisher(Observable.empty()); // temporary until fix birectional
		// connect the Responder
		Publisher<Void> responderConnectionHandler = toPublisher(Observable.empty()); // temporary until fix birectional

		if(isServer) {
			responderConnectionHandler = responder.acceptConnection(connection);
		} else {
			requester = Requester.createForConnection(isServer, connection);
			requesterConnectionHandler = requester.start();
		}
		
		// merge Publishers and subscribe to start processing messages and handle errors
		return toPublisher(Observable.merge(toObservable(requesterConnectionHandler), toObservable(responderConnectionHandler))
				.doOnSubscribe(() -> System.out.println("ReactiveSocket Starting " + (isServer ? "Server" : "Client")))
				.doOnUnsubscribe(() -> System.out.println("ReactiveSocket Shutting Down" + (isServer ? "Server" : "Client")))
				.doOnCompleted(() -> System.out.println("ReactiveSocket Complete" + (isServer ? "Server" : "Client")))
				.doOnError(t -> System.out.println("ReactiveSocket Error " + t.getMessage() + " " + (isServer ? "Server" : "Client"))));
	}
}
