# ReactiveSocket

[![Join the chat at https://gitter.im/ReactiveSocket/reactivesocket-java](https://badges.gitter.im/ReactiveSocket/reactivesocket-java.svg)](https://gitter.im/ReactiveSocket/reactivesocket-java?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

ReactiveSocket is network protocol with client and server that uses [Reactive Streams](http://reactive-streams.org) (and optimistically [Reactive Streams IO](http://reactive-streams.io) as it gets defined).

It enables the following interaction models via async message passing over a single network connection:

- request/response (stream of 1)
- request/stream (finite stream of many)
- fire-and-forget (no response)
- event subscription (infinite stream of many)

This is the core project for Java that implements the protocol and exposes Reactive Stream APIs. Typically most use will come via another library that uses this one.

For example:

- ReactiveSocket over WebSockets using Netty [reactivesocket-websockets-netty](https://github.com/ReactiveSocket/reactivesocket-java-impl)
- ReactiveSocket over TCP using Netty [reactivesocket-tcp-netty](https://github.com/ReactiveSocket/reactivesocket-java-impl)
- ReactiveSocket over Aeron using Aeron Java [reactivesocket-aeron-java](https://github.com/ReactiveSocket/reactivesocket-java-impl)

ReactiveSocket is for communicating across network boundaries thus it is intended to be polyglot. Common libraries include:

- ReactiveSocket over WebSockets using Javascript (for Node.js and browsers) [reactivesocket-websockets-javascript](https://github.com/ReactiveSocket/reactivesocket-websockets-javascript)

Others can be found in the [ReactiveSocket Github](https://github.com/ReactiveSocket) project.

## Build and Binaries

<a href='https://travis-ci.org/ReactiveSocket/reactivesocket-java/builds'><img src='https://travis-ci.org/ReactiveSocket/reactivesocket-java.svg?branch=master'></a>

Snapshots are available via JFrog.

Example:

```groovy
repositories {
    maven { url 'https://oss.jfrog.org/libs-snapshot' }
}

dependencies {
    compile 'io.reactivesocket:reactivesocket:0.5.0-SNAPSHOT'
}
```

No releases to Maven Central or JCenter have occurred yet.


## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/ReactiveSocket/reactivesocket-java/issues).

## LICENSE

Copyright 2015 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
