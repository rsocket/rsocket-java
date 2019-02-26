package io.rsocket.resume;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class ResumeCalculatorTest {

  private ResumedFramesCalculator clientResumeCalculator;
  private ResumedFramesCalculator serverResumeCalculator;

  @BeforeEach
  void setUp() {
    clientResumeCalculator = ResumedFramesCalculator.ofClient;
    serverResumeCalculator = ResumedFramesCalculator.ofServer;
  }

  @Test
  void clientResumeSuccess() {
    ResumptionState local = ResumptionState.fromClient(1, 42);
    ResumptionState remote = ResumptionState.fromServer(3);
    StepVerifier.create(clientResumeCalculator.calculate(local, remote))
        .expectNext(3L)
        .expectComplete()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void clientResumeError() {
    ResumptionState local = ResumptionState.fromClient(4, 42);
    ResumptionState remote = ResumptionState.fromServer(3);
    StepVerifier.create(clientResumeCalculator.calculate(local, remote))
        .expectError(ResumeStateException.class)
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void serverResumeSuccess() {
    ResumptionState local = ResumptionState.fromClient(1, 42);
    ResumptionState remote = ResumptionState.fromClient(4, 23);
    StepVerifier.create(serverResumeCalculator.calculate(local, remote))
        .expectNext(23L)
        .expectComplete()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void serverResumeErrorClientState() {
    ResumptionState local = ResumptionState.fromClient(1, 3);
    ResumptionState remote = ResumptionState.fromClient(4, 23);
    StepVerifier.create(serverResumeCalculator.calculate(local, remote))
        .expectError(ResumeStateException.class)
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void serverResumeErrorServerState() {
    ResumptionState local = ResumptionState.fromClient(4, 42);
    ResumptionState remote = ResumptionState.fromClient(4, 1);
    StepVerifier.create(serverResumeCalculator.calculate(local, remote))
        .expectError(ResumeStateException.class)
        .verify(Duration.ofSeconds(1));
  }
}
