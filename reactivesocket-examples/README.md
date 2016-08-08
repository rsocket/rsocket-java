# ReactiveSocket Examples

## Sample App

Inside the `sampleapp` package is an extremely bare bones example of a ReactiveSocket server and a ReactiveSocket client.
This can be used as an example for anyone looking to implement either an RS client or server.

## Kafka

Inside the `kafkaclient` and `kafkaserver` packages are bare bones implementation of both an Apache Kafka-like application
as well as a simple IRC like chat application as well. Users can just run the server and then run the client, and use
the client to interact with the server. The client has run instructions.

The Kafka-like application allows users to create named streams with a request-response call. Users can subscribe to
streams, and all updates to that stream object will be live; this is done using a request-stream. Users can choose to publish
to streams, this is done using a fire-and-forget. Finally, in our IRC like application, if we have multiple clients connected to our server,
they can all chat with each over over our IRC. This is done using a request-channel, where the client to server stream is
all the messages that each client want to send, and the server to client stream is all the messages all the other clients have
sent. This chat application is much easier and cleaner to build, and much more responsive than any pull based chat client.