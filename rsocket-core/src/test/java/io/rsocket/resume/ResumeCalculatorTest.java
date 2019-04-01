/*
 * Copyright 2015-2019 the original author or authors.
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
    StepVerifier.create(clientResumeCalculator.calculate(1, 42, -1, 3))
        .expectNext(3L)
        .expectComplete()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void clientResumeError() {
    StepVerifier.create(clientResumeCalculator.calculate(4, 42, -1, 3))
        .expectError(ResumeStateException.class)
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void serverResumeSuccess() {
    StepVerifier.create(serverResumeCalculator.calculate(1, 42, 4, 23))
        .expectNext(23L)
        .expectComplete()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void serverResumeErrorClientState() {
    StepVerifier.create(serverResumeCalculator.calculate(1, 3, 4, 23))
        .expectError(ResumeStateException.class)
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void serverResumeErrorServerState() {
    StepVerifier.create(serverResumeCalculator.calculate(4, 42, 4, 1))
        .expectError(ResumeStateException.class)
        .verify(Duration.ofSeconds(1));
  }
}
