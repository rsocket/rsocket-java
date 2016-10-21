## Overview

This module provides support for encoding/decoding ReactiveSocket data and metadata into using different mime types as defined by [ReactiveSocket protocol](https://github.com/ReactiveSocket/reactivesocket/blob/master/Protocol.md#sendSetupFrame-frame).
The support for mime types is not comprehensive but it will at least support the [default metadata mime type](https://github.com/ReactiveSocket/reactivesocket/blob/mimetypes/MimeTypes.md)

## Usage

#### Supported Codecs

Supported mime types are listed as [SupportedMimeTypes](src/main/java/io/reactivesocket/mimetypes/SupportedMimeTypes.java).

#### Obtaining the appropriate codec

[MimeType](src/main/java/io/reactivesocket/mimetypes/MimeType.java) is the interface that provides different methods for encoding/decoding ReactiveSocket data and metadata.
An instance of `MimeType` can be obtained via [MimeTypeFactory](src/main/java/io/reactivesocket/mimetypes/MimeTypeFactory.java).

A simple usage of `MimeType` is as follows:

```java
public class ConnectionSetupHandlerImpl implements ConnectionSetupHandler {

    @Override
    public RequestHandler apply(ConnectionSetupPayload setupPayload, ReactiveSocket reactiveSocket) throws SetupException {

        final MimeType mimeType = MimeTypeFactory.from(setupPayload); // If the mime types aren't supported, throws an error.

        return new RequestHandler() {

            // Not a complete implementation, just a method to demonstrate usage.
            @Override
            public Publisher<Payload> handleRequestResponse(Payload payload) {
                // use (en/de)codeMetadata() methods to encode/decode metadata
                mimeType.decodeMetadata(payload.getMetadata(), KVMetadata.class);
                // use (en/de)codeData() methods to encode/decode data
                mimeType.decodeData(payload.getData(), Person.class);
                return PublisherUtils.empty(); // Do something useful in reality!
            }
        };
    }
}
```

## Build and Binaries

<a href='https://travis-ci.org/ReactiveSocket/reactivesocket-java/builds'><img src='https://travis-ci.org/ReactiveSocket/reactivesocket-java.svg?branch=master'></a>

Artifacts are available via JCenter.

Example:

```groovy
repositories {
    maven { url 'https://jcenter.bintray.com' }
}

dependencies {
    compile 'io.reactivesocket:reactivesocket-mime-types:x.y.z'
}
```

No releases to Maven Central have occurred yet.


## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/ReactiveSocket/reactivesocket-java-impl/issues).
