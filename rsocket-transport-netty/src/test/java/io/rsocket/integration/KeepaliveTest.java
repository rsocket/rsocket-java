package io.rsocket.integration;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketClient;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

public class KeepaliveTest {

	private static final Logger LOG = LoggerFactory.getLogger(KeepaliveTest.class);
	private static final int PORT = 23200;

	@Test
	void keepAliveTest() {
		createServer().block();
		RSocketClient rsocketClient = createClient();

		int expectedCount = 4;
		AtomicBoolean sleepOnce = new AtomicBoolean(true);
		StepVerifier.create(
						Flux.range(0, expectedCount)
								.delayElements(Duration.ofMillis(2000))
								.concatMap(i ->
										rsocketClient.requestResponse(Mono.just(DefaultPayload.create("")))
												.doOnNext(__ -> {
													if (sleepOnce.getAndSet(false)) {
														try {
															LOG.info("Sleeping...");
															Thread.sleep(1_000);
															LOG.info("Waking up.");
														} catch (InterruptedException e) {
															throw new RuntimeException(e);
														}
													}
												})
												.log("id " + i)
												.onErrorComplete()
								))
				.expectSubscription()
				.expectNextCount(expectedCount)
				.verifyComplete();
	}

	@Test
	void keepAliveTestLazy() {
		createServer().block();
		Mono<RSocket> rsocketMono = createClientLazy();

		int expectedCount = 4;
		AtomicBoolean sleepOnce = new AtomicBoolean(true);
		StepVerifier.create(
				            Flux.range(0, expectedCount)
				                .delayElements(Duration.ofMillis(2000))
				                .concatMap(i ->
						                rsocketMono.flatMap(rsocket -> rsocket.requestResponse(DefaultPayload.create(""))
						                             .doOnNext(__ -> {
							                             if (sleepOnce.getAndSet(false)) {
								                             try {
									                             LOG.info("Sleeping...");
									                             Thread.sleep(1_000);
									                             LOG.info("Waking up.");
								                             } catch (InterruptedException e) {
									                             throw new RuntimeException(e);
								                             }
							                             }
						                             })
						                             .log("id " + i)
						                             .onErrorComplete()
						                )
				                ))
		            .expectSubscription()
		            .expectNextCount(expectedCount)
		            .verifyComplete();
	}

	private static Mono<CloseableChannel> createServer() {
		LOG.info("Starting server at port {}", PORT);

		TcpServer tcpServer = TcpServer.create().host("localhost").port(PORT);

		return RSocketServer.create((setupPayload, rSocket) -> {
					rSocket.onClose()
							.doFirst(() -> LOG.info("Connected on server side."))
							.doOnTerminate(() -> LOG.info("Connection closed on server side."))
							.subscribe();

					return Mono.just(new MyServerRsocket());
				})
				.payloadDecoder(PayloadDecoder.ZERO_COPY)
				.bind(TcpServerTransport.create(tcpServer))
				.doOnNext(closeableChannel -> LOG.info("RSocket server started."));
	}

	private static RSocketClient createClient() {
		LOG.info("Connecting....");

		Function<String, RetryBackoffSpec> reconnectSpec = reason -> Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(10L))
				.doBeforeRetry(retrySignal -> LOG.info("Reconnecting. Reason: {}", reason));

		Mono<RSocket> rsocketMono = RSocketConnector.create()
				.fragment(16384)
				.reconnect(reconnectSpec.apply("connector-close"))
				.keepAlive(Duration.ofMillis(100L), Duration.ofMillis(900L))
				.connect(TcpClientTransport.create(TcpClient.create().host("localhost").port(PORT)));

		RSocketClient client = RSocketClient.from(rsocketMono);

		client
				.source()
				.doOnNext(r -> LOG.info("Got RSocket"))
				.flatMap(RSocket::onClose)
				.doOnError(err -> LOG.error("Error during onClose.", err))
				.retryWhen(reconnectSpec.apply("client-close"))
				.doFirst(() -> LOG.info("Connected on client side."))
				.doOnTerminate(() -> LOG.info("Connection closed on client side."))
				.repeat()
				.subscribe();

		return client;
	}


	private static Mono<RSocket> createClientLazy() {
		LOG.info("Connecting....");

		Function<String, RetryBackoffSpec> reconnectSpec = reason -> Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(10L))
		                                                                  .doBeforeRetry(retrySignal -> LOG.info("Reconnecting. Reason: {}", reason));

		return RSocketConnector.create()
		                                            .fragment(16384)
		                                            .reconnect(reconnectSpec.apply("connector-close"))
		                                            .keepAlive(Duration.ofMillis(100L), Duration.ofMillis(900L))
		                                            .connect(TcpClientTransport.create(TcpClient.create().host("localhost").port(PORT)));

//		RSocketClient client = RSocketClient.from(rsocketMono);

//		client
//				.source()
//				.doOnNext(r -> LOG.info("Got RSocket"))
//				.flatMap(RSocket::onClose)
//				.doOnError(err -> LOG.error("Error during onClose.", err))
//				.retryWhen(reconnectSpec.apply("client-close"))
//				.doFirst(() -> LOG.info("Connected on client side."))
//				.doOnTerminate(() -> LOG.info("Connection closed on client side."))
//				.repeat()
//				.subscribe();

//		return client;
	}

	public static class MyServerRsocket implements RSocket {

		@Override
		public Mono<Payload> requestResponse(Payload payload) {
			return Mono.just("Pong").map(DefaultPayload::create);
		}
	}
}