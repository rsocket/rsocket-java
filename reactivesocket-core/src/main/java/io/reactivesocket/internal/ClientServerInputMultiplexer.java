/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.internal;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Plugins;
import org.agrona.BitUtil;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import static io.reactivesocket.Plugins.NOOP_ASYNC_FRAME_INTERCEPTOR;
import static io.reactivesocket.Plugins.NOOP_FRAME_INTERCEPTOR;

/**
 * {@link DuplexConnection#receive()} is a single stream on which the following type of frames arrive:
 * <ul>
 <li>Frames for streams initiated by the initiator of the connection (client).</li>
 <li>Frames for streams initiated by the acceptor of the connection (server).</li>
 </ul>
 *
 * The only way to differentiate these two frames is determining whether the stream Id is odd or even. Even IDs are
 * for the streams initiated by server and odds are for streams initiated by the client. <p>
 */
public class ClientServerInputMultiplexer {
    private static final Logger LOGGER = LoggerFactory.getLogger("io.reactivesocket.FrameLogger");

    private final InternalDuplexConnection streamZeroConnection;
    private final InternalDuplexConnection serverConnection;
    private final InternalDuplexConnection clientConnection;

    public enum Type { STREAM_ZERO, CLIENT, SERVER }

    public ClientServerInputMultiplexer(DuplexConnection source) {
        final MonoProcessor<Flux<Frame>> streamZero = MonoProcessor.create();
        final MonoProcessor<Flux<Frame>> server = MonoProcessor.create();
        final MonoProcessor<Flux<Frame>> client = MonoProcessor.create();

        streamZeroConnection = new InternalDuplexConnection(source, streamZero);
        serverConnection = new InternalDuplexConnection(source, server);
        clientConnection = new InternalDuplexConnection(source, client);

        source.receive()
            .groupBy(frame -> {
                int streamId = frame.getStreamId();
                Type type;
                if (streamId == 0) {
                    type = Type.STREAM_ZERO;
                } else if (BitUtil.isEven(streamId)) {
                    type = Type.SERVER;
                } else {
                    type = Type.CLIENT;
                }
                return type;
            })
            .subscribe(group -> {
                Flux<Frame> frames = group;
                switch (group.key()) {
                    case STREAM_ZERO:
                        if (Plugins.FRAME_INTERCEPTOR != NOOP_FRAME_INTERCEPTOR) {
                            frames = group
                                .map(Plugins.FRAME_INTERCEPTOR.apply(Type.STREAM_ZERO)::apply);
                        }

                        if (Plugins.ASYNC_FRAME_INTERCEPTOR != NOOP_ASYNC_FRAME_INTERCEPTOR) {
                            frames = frames
                                .flatMap(Plugins.ASYNC_FRAME_INTERCEPTOR.apply(Type.STREAM_ZERO)::apply);
                        }

                        streamZero.onNext(frames);
                        break;

                    case SERVER:
                        if (Plugins.FRAME_INTERCEPTOR != NOOP_FRAME_INTERCEPTOR) {
                            frames = group
                                .map(Plugins.FRAME_INTERCEPTOR.apply(Type.SERVER)::apply);
                        }

                        if (Plugins.ASYNC_FRAME_INTERCEPTOR != NOOP_ASYNC_FRAME_INTERCEPTOR) {
                            frames = frames
                                .flatMap(Plugins.ASYNC_FRAME_INTERCEPTOR.apply(Type.SERVER)::apply);
                        }

                        server.onNext(frames);
                        break;

                    case CLIENT:
                        if (Plugins.FRAME_INTERCEPTOR != NOOP_FRAME_INTERCEPTOR) {
                            frames = group
                                .map(Plugins.FRAME_INTERCEPTOR.apply(Type.CLIENT)::apply);
                        }

                        if (Plugins.ASYNC_FRAME_INTERCEPTOR != NOOP_ASYNC_FRAME_INTERCEPTOR) {
                            frames = frames
                                .flatMap(Plugins.ASYNC_FRAME_INTERCEPTOR.apply(Type.CLIENT)::apply);
                        }

                        client.onNext(frames);
                        break;
                }
            });
    }

    public DuplexConnection asServerConnection() {
        return serverConnection;
    }

    public DuplexConnection asClientConnection() {
        return clientConnection;
    }

    public DuplexConnection asStreamZeroConnection() {
        return streamZeroConnection;
    }

    private static class InternalDuplexConnection implements DuplexConnection {
        private final DuplexConnection source;
        private final MonoProcessor<Flux<Frame>> processor;
        private final boolean debugEnabled;

        public InternalDuplexConnection(DuplexConnection source, MonoProcessor<Flux<Frame>> processor) {
            this.source = source;
            this.processor = processor;
            this.debugEnabled = LOGGER.isDebugEnabled();
        }

        @Override
        public Mono<Void> send(Publisher<Frame> frame) {
            if (debugEnabled) {
                frame = Flux.from(frame).doOnNext(f -> LOGGER.debug("sending -> " + f.toString()));
            }

            return source.send(frame);
        }

        @Override
        public Mono<Void> sendOne(Frame frame) {
            if (debugEnabled) {
                LOGGER.debug("sending -> " + frame.toString());
            }

            return source.sendOne(frame);
        }

        @Override
        public Flux<Frame> receive() {
            return processor.flatMap(f -> {
                if (debugEnabled) {
                    return f.doOnNext(frame -> LOGGER.debug("receiving -> " + frame.toString()));
                } else {
                    return f;
                }
            });
        }

        @Override
        public Mono<Void> close() {
            return source.close();
        }

        @Override
        public Mono<Void> onClose() {
            return source.onClose();
        }

        @Override
        public double availability() {
            return source.availability();
        }
    }

}
