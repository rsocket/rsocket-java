package io.rsocket;

public class KeepAliveTest {
  /*private static final int CLIENT_REQUESTER_TICK_PERIOD = 100;
  private static final int CLIENT_REQUESTER_TIMEOUT = 700;
  private static final int CLIENT_REQUESTER_MISSED_ACKS = 3;
  private static final int SERVER_RESPONDER_TICK_PERIOD = 100;
  private static final int SERVER_RESPONDER_TIMEOUT = 1000;

  @ParameterizedTest
  @MethodSource("testData")
  void keepAlives(Supplier<TestData> testDataSupplier) {
    TestData testData = testDataSupplier.get();
    TestDuplexConnection connection = testData.connection();

    Flux.interval(Duration.ofMillis(100))
        .subscribe(
            n -> connection.addToReceivedBuffer(Frame.Keepalive.from(Unpooled.EMPTY_BUFFER, true)));

    Mono.delay(Duration.ofMillis(1500)).block();

    RSocket rSocket = testData.rSocket();
    List<Throwable> errors = testData.errors().errors();

    Assertions.assertThat(rSocket.isDisposed()).isFalse();
    Assertions.assertThat(errors).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("testData")
  void keepAlivesMissing(Supplier<TestData> testDataSupplier) {
    TestData testData = testDataSupplier.get();
    RSocket rSocket = testData.rSocket();

    Mono.delay(Duration.ofMillis(1500)).block();

    List<Throwable> errors = testData.errors().errors();
    Assertions.assertThat(rSocket.isDisposed()).isTrue();
    Assertions.assertThat(errors).hasSize(1);
    Throwable throwable = errors.get(0);
    Assertions.assertThat(throwable).isInstanceOf(ConnectionErrorException.class);
  }

  @Test
  void clientRequesterRespondsToKeepAlives() {
    TestData testData = requester(100, 700, 3).get();
    TestDuplexConnection connection = testData.connection();

    Mono.delay(Duration.ofMillis(100))
        .subscribe(
            l -> connection.addToReceivedBuffer(Frame.Keepalive.from(Unpooled.EMPTY_BUFFER, true)));

    Mono<Void> keepAliveResponse =
        Flux.from(connection.getSentAsPublisher())
            .filter(f -> f.getType() == FrameType.KEEPALIVE && !Frame.Keepalive.hasRespondFlag(f))
            .next()
            .then();

    StepVerifier.create(keepAliveResponse).expectComplete().verify(Duration.ofSeconds(5));
  }

  static Stream<Supplier<TestData>> testData() {
    return Stream.of(
        requester(
            CLIENT_REQUESTER_TICK_PERIOD, CLIENT_REQUESTER_TIMEOUT, CLIENT_REQUESTER_MISSED_ACKS),
        responder(SERVER_RESPONDER_TICK_PERIOD, SERVER_RESPONDER_TIMEOUT));
  }

  static Supplier<TestData> requester(int tickPeriod, int timeout, int missedAcks) {
    return () -> {
      TestDuplexConnection connection = new TestDuplexConnection();
      Errors errors = new Errors();
      RSocketClient rSocket =
          new RSocketClient(
              connection,
              DefaultPayload::create,
              errors,
              StreamIdSupplier.clientSupplier(),
              Duration.ofMillis(tickPeriod),
              Duration.ofMillis(timeout),
              missedAcks);
      return new TestData(rSocket, errors, connection);
    };
  }

  static Supplier<TestData> responder(int tickPeriod, int timeout) {
    return () -> {
      TestDuplexConnection connection = new TestDuplexConnection();
      AbstractRSocket handler = new AbstractRSocket() {};
      Errors errors = new Errors();
      RSocketServer rSocket =
          new RSocketServer(
              connection, handler, DefaultPayload::create, errors, tickPeriod, timeout);
      return new TestData(rSocket, errors, connection);
    };
  }

  static class TestData {
    private final RSocket rSocket;
    private final Errors errors;
    private final TestDuplexConnection connection;

    public TestData(RSocket rSocket, Errors errors, TestDuplexConnection connection) {
      this.rSocket = rSocket;
      this.errors = errors;
      this.connection = connection;
    }

    public TestDuplexConnection connection() {
      return connection;
    }

    public RSocket rSocket() {
      return rSocket;
    }

    public Errors errors() {
      return errors;
    }
  }

  static class Errors implements Consumer<Throwable> {
    private final List<Throwable> errors = new ArrayList<>();

    @Override
    public void accept(Throwable throwable) {
      errors.add(throwable);
    }

    public List<Throwable> errors() {
      return new ArrayList<>(errors);
    }
  }*/
}
