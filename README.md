# RSocket

[![Join the chat at https://gitter.im/RSocket/reactivesocket-java](https://badges.gitter.im/RSocket/reactivesocket-java.svg)](https://gitter.im/ReactiveSocket/reactivesocket-java)

RSocket is a binary protocol for use on byte stream transports such as TCP, WebSockets, and Aeron.

It enables the following symmetric interaction models via async message passing over a single connection:

- request/response (stream of 1)
- request/stream (finite stream of many)
- fire-and-forget (no response)
- event subscription (infinite stream of many)

Learn more at http://rsocket.io

## Build and Binaries

[![Build Status](https://travis-ci.org/rsocket/rsocket-java.svg?branch=develop)](https://travis-ci.org/rsocket/rsocket-java)

Releases are available via Maven Central.

Example:

```groovy
dependencies {
    implementation 'io.rsocket:rsocket-core:0.12.2-RC4'
    implementation 'io.rsocket:rsocket-transport-netty:0.12.2-RC4'
//    implementation 'io.rsocket:rsocket-core:1.0.0-RC1-SNAPSHOT'
//    implementation 'io.rsocket:rsocket-transport-netty:1.0.0-RC1-SNAPSHOT'
}
```


## Development

Install the google-java-format in Intellij, from Plugins preferences.
Enable under Preferences -> Other Settings -> google-java-format Settings

Format automatically with

```
$./gradlew goJF
```

## Debugging
Frames can be printed out to help debugging. Set the logger `io.rsocket.FrameLogger` to debug to print the frames.

## Requirements

- Java 8 - heavy dependence on Java 8 functional APIs and java.time, also on Reactor
- Android O - https://github.com/rsocket/rsocket-demo-android-java8

## Trivial Client

```
package io.rsocket.transport.netty;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;

import java.net.URI;

public class ExampleClient {
    public static void main(String[] args) {
        WebsocketClientTransport ws = WebsocketClientTransport.create(URI.create("ws://rsocket-demo.herokuapp.com/ws"));
        RSocket client = RSocketFactory.connect().keepAlive().transport(ws).start().block();

        try {
            Flux<Payload> s = client.requestStream(DefaultPayload.create("peace"));

            s.take(10).doOnNext(p -> System.out.println(p.getDataUtf8())).blockLast();
        } finally {
            client.dispose();
        }
    }
}
```

## Zero Copy
By default to make RSocket easier to use it copies the incoming Payload. Copying the payload comes at cost to performance
and latency. If you want to use zero copy you must disable this. To disable copying you must include a `payloadDecoder`
argument in your `RSocketFactory`. This will let you manage the Payload without copying the data from the underlying
transport. You must free the Payload when you are done with them
or you will get a memory leak. Used correctly this will reduce latency and increase performance.

### Example Server setup
```java
RSocketFactory.receive()
        // Enable Zero Copy
        .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor(new PingHandler())
        .transport(TcpServerTransport.create(7878))
        .start()
        .block()
        .onClose()
        .block();
```

### Example Client setup
```java
Mono<RSocket> client =
        RSocketFactory.connect()
            // Enable Zero Copy
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(TcpClientTransport.create(7878))
            .start();
```

## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/RSocket/reactivesocket-java/issues).

## LICENSE

Copyright 2015-2018 the original author or authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
