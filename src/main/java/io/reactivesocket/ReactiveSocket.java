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

import static rx.Observable.error;
import static rx.RxReactiveStreams.toPublisher;

/**
 * Interface for a connection that supports sending requests and receiving responses
 *
 * Created by servers for connections
 * Created on demand for clients
 */
public class ReactiveSocket
{
    private static final Publisher<String> NOT_FOUND_ERROR_STRING = toPublisher(error(new Exception("Not Found!")));
    private static final Publisher<Void> NOT_FOUND_ERROR_VOID = toPublisher(error(new Exception("Not Found!")));
    private static final RequestHandler EMPTY_HANDLER = new RequestHandler()
    {
        public Publisher<String> handleRequestResponse(String request)
        {
            return NOT_FOUND_ERROR_STRING;
        }

        public Publisher<String> handleRequestStream(String request)
        {
            return NOT_FOUND_ERROR_STRING;
        }

        public Publisher<String> handleRequestSubscription(String request)
        {
            return NOT_FOUND_ERROR_STRING;
        }

        public Publisher<Void> handleFireAndForget(String request)
        {
            return NOT_FOUND_ERROR_VOID;
        }
    };

    private final Requester requester;
    private final Publisher<Void> responderPublisher;

    public static ReactiveSocket connect(final DuplexConnection connection)
    {
        return connect(connection, EMPTY_HANDLER);
    }

    /**
     * Create a ReactiveSocket and initiate connect processing as a client
     *
     * @param connection
     * @param requestHandler
     * @return
     */
    public static ReactiveSocket connect(final DuplexConnection connection, final RequestHandler requestHandler)
    {
        final ReactiveSocket socket = new ReactiveSocket(connection, Responder.create(requestHandler));

        // TODO: initiate connect logic as a client

        return socket;
    }

    /**
     * Create a ReactiveSocket and prepare for operation as a server
     *
     * @param connection
     * @param requestHandler
     * @return
     */
    public static ReactiveSocket accept(final DuplexConnection connection, final RequestHandler requestHandler)
    {
        final ReactiveSocket socket = new ReactiveSocket(connection, Responder.create(requestHandler));

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
    public Publisher<String> requestResponse(final String data, final String metadata)
    {
        return requester.requestResponse(data);
    }

    public Publisher<Void> fireAndForget(final String data, final String metadata)
    {
        return requester.fireAndForget(data);
    }

    public Publisher<String> requestStream(final String data, final String metadata)
    {
        return requester.requestStream(data);
    }

    public Publisher<String> requestSubscription(final String data, final String metadata)
    {
        return requester.requestSubscription(data);
    }

    public Publisher<Void> responderPublisher()
    {
        return responderPublisher;
    }

    private ReactiveSocket(final DuplexConnection connection, final Responder responder)
    {
        this.requester = Requester.create(connection);
        this.responderPublisher = responder.acceptConnection(connection);
    }
}
