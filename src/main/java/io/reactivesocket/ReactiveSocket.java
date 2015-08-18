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
import org.reactivestreams.Subscriber;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

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

    private final Requester requester;
    private final Publisher<Void> responderPublisher;

    private final Long2ObjectHashMap<UnicastSubject> requesterStreamInputMap = new Long2ObjectHashMap<>();

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
        final ReactiveSocket socket = new ReactiveSocket(connection, requestHandler);

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
        final ReactiveSocket socket = new ReactiveSocket(connection, requestHandler);

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

    public Publisher<Void> responderPublisher()
    {
        return responderPublisher;
    }

    private ReactiveSocket(final DuplexConnection connection, final RequestHandler requestHandler)
    {
        this.requester = Requester.create(connection, requesterStreamInputMap);
        this.responderPublisher = Responder.create(requestHandler).acceptConnection(connection);

        connection.getInput().subscribe(new Subscriber<Frame>()
        {
            public void onSubscribe(Subscription s)
            {
                s.request(Long.MAX_VALUE);
            }

            public void onNext(Frame frame)
            {
                if (frame.getType().isRequestType())
                {
                    requesterStreamInputMap.get(frame.getStreamId()).onNext(frame);
                }
                else
                {

                }
            }

            public void onError(Throwable t)
            {
                requesterStreamInputMap.forEach((id, subject) -> subject.onError(t));
                // TODO: iterate over responder side and destroy world
            }

            public void onComplete()
            {
                // TODO: might be a RuntimeException
                requesterStreamInputMap.forEach((id, subject) -> subject.onCompleted());
                // TODO: iterate over responder side and destroy world
            }
        });

    }
}
