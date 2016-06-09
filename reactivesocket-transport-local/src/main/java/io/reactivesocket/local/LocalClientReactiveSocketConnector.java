/**
 * Copyright 2016 Netflix, Inc.
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
package io.reactivesocket.local;

import io.reactivesocket.*;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;

public class LocalClientReactiveSocketConnector implements ReactiveSocketConnector<LocalClientReactiveSocketConnector.Config> {
    public static final LocalClientReactiveSocketConnector INSTANCE = new LocalClientReactiveSocketConnector();

    private LocalClientReactiveSocketConnector() {}

    @Override
    public Publisher<ReactiveSocket> connect(Config config) {
        return s -> {
            try {
                s.onSubscribe(EmptySubscription.INSTANCE);
                LocalClientDuplexConnection clientConnection = LocalReactiveSocketManager
                    .getInstance()
                    .getClientConnection(config.getName());
                ReactiveSocket reactiveSocket = DefaultReactiveSocket
                    .fromClientConnection(clientConnection, ConnectionSetupPayload.create(config.getMetadataMimeType(), config.getDataMimeType()));

                reactiveSocket.startAndWait();

                s.onNext(reactiveSocket);
                s.onComplete();
            } catch (Throwable t) {
                s.onError(t);
            }
        };
    }

    public static class Config {
        final String name;
        final String metadataMimeType;
        final String dataMimeType;

        public Config(String name, String metadataMimeType, String dataMimeType) {
            this.name = name;
            this.metadataMimeType = metadataMimeType;
            this.dataMimeType = dataMimeType;
        }

        public String getName() {
            return name;
        }

        public String getMetadataMimeType() {
            return metadataMimeType;
        }

        public String getDataMimeType() {
            return dataMimeType;
        }
    }
}
