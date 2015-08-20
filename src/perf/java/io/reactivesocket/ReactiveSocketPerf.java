package io.reactivesocket;

import static rx.Observable.*;
import static rx.RxReactiveStreams.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ReactiveSocketPerf {

	@Benchmark
	public void requestResponseHello(Input input) {
		Input.client.requestResponse(Input.HELLO_PAYLOAD).subscribe(input.latchedSubscriber);
	}

	@Benchmark
	public void requestStreamHello1000(Input input) {
		Input.client.requestStream(Input.HELLO_PAYLOAD).subscribe(input.latchedSubscriber);
	}

	@State(Scope.Thread)
	public static class Input {
		/**
		 * Use to consume values when the test needs to return more than a single value.
		 */
		public Blackhole bh;

		static final ByteBuffer HELLO = ByteBuffer.wrap("HELLO".getBytes());
		static final ByteBuffer HELLO_WORLD = ByteBuffer.wrap("HELLO_WORLD".getBytes());
		static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

		static final Payload HELLO_PAYLOAD = new Payload() {

			@Override
			public ByteBuffer getMetadata() {
				return EMPTY;
			}

			@Override
			public ByteBuffer getData() {
				HELLO.position(0);
				return HELLO;
			}
		};

		static final Payload HELLO_WORLD_PAYLOAD = new Payload() {

			@Override
			public ByteBuffer getMetadata() {
				return EMPTY;
			}

			@Override
			public ByteBuffer getData() {
				HELLO_WORLD.position(0);
				return HELLO_WORLD;
			}
		};

		final static PerfTestConnection serverConnection = new PerfTestConnection();
		final static PerfTestConnection clientConnection = new PerfTestConnection();

		static {
			clientConnection.connectToServerConnection(serverConnection);
		}

		final static ReactiveSocket serverSocket = ReactiveSocket.createResponderAndRequestor(new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				return toPublisher(just(HELLO_WORLD_PAYLOAD));
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				return toPublisher(just(HELLO_WORLD_PAYLOAD).repeat(1000));
			}

			@Override
			public Publisher<Payload> handleRequestSubscription(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

		});

		final static ReactiveSocket client = ReactiveSocket.createRequestor();

		static {
			// start both the server and client and monitor for errors
			toObservable(serverSocket.connect(serverConnection)).subscribe();
			toObservable(client.connect(clientConnection)).subscribe();
		}

		LatchedSubscriber<Payload> latchedSubscriber;

		@Setup
		public void setup(Blackhole bh) {
			this.bh = bh;
			latchedSubscriber = new LatchedSubscriber<Payload>(bh);
		}
	}
}
