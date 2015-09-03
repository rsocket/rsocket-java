package io.reactivesocket;

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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.internal.PublisherUtils;
import io.reactivesocket.perfutil.PerfTestConnection;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ReactiveSocketPerf {

	@Benchmark
	public void requestResponseHello(Input input) {
		// this is synchronous so we don't need to use a CountdownLatch to wait
		Input.client.requestResponse(Input.HELLO_PAYLOAD).subscribe(input.blackholeConsumer);
	}

	@Benchmark
	public void requestStreamHello1000(Input input) {
		// this is synchronous so we don't need to use a CountdownLatch to wait
		Input.client.requestStream(Input.HELLO_PAYLOAD).subscribe(input.blackholeConsumer);
	}
	
	@Benchmark
	public void fireAndForgetHello(Input input) {
		// this is synchronous so we don't need to use a CountdownLatch to wait
		Input.client.fireAndForget(Input.HELLO_PAYLOAD).subscribe(input.voidBlackholeConsumer);
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

		private static Publisher<Payload> HELLO_1 = just(HELLO_WORLD_PAYLOAD);
		private static Publisher<Payload> HELLO_1000;

		static {
			Payload[] ps = new Payload[1000];
			for (int i = 0; i < ps.length; i++) {
				ps[i] = HELLO_WORLD_PAYLOAD;
			}
			HELLO_1000 = just(ps);
		}

		static final RequestHandler handler = new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				return HELLO_1;
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				return HELLO_1000;
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				return null;
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return PublisherUtils.empty();
			}

			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
				return null;
			}

		};

		final static ReactiveSocket serverSocket = ReactiveSocket.fromServerConnection(serverConnection, setupFrame -> handler);

		final static ReactiveSocket client =
			ReactiveSocket.fromClientConnection(
				clientConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS), t -> {});

		static {
			serverSocket.start();
			client.start();
		}

		Subscriber<Payload> blackholeConsumer; // reuse this each time
		Subscriber<Void> voidBlackholeConsumer; // reuse this each time

		@Setup
		public void setup(Blackhole bh) {
			this.bh = bh;
			blackholeConsumer = new Subscriber<Payload>() {

				@Override
				public void onSubscribe(Subscription s) {
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(Payload t) {
					bh.consume(t);
				}

				@Override
				public void onError(Throwable t) {
					t.printStackTrace();
				}

				@Override
				public void onComplete() {

				}

			};
			
			voidBlackholeConsumer = new Subscriber<Void>() {

				@Override
				public void onSubscribe(Subscription s) {
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(Void t) {
				}

				@Override
				public void onError(Throwable t) {
					t.printStackTrace();
				}

				@Override
				public void onComplete() {

				}

			};
		}
	}

	private static Publisher<Payload> just(Payload... ps) {
		return new Publisher<Payload>() {

			@Override
			public void subscribe(Subscriber<? super Payload> s) {
				s.onSubscribe(new Subscription() {

					int emitted = 0;

					@Override
					public void request(long n) {
						// NOTE: This is not a safe implementation as it assumes synchronous request(n)
						for (int i = 0; i < n; i++) {
							s.onNext(ps[emitted++]);
							if (emitted == ps.length) {
								s.onComplete();
								break;
							}
						}
					}

					@Override
					public void cancel() {

					}

				});
			}

		};
	}

	private static class ErrorSubscriber<T> implements Subscriber<T> {

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(T t) {

		}

		@Override
		public void onError(Throwable t) {
			t.printStackTrace();
		}

		@Override
		public void onComplete() {

		}

	}

}
