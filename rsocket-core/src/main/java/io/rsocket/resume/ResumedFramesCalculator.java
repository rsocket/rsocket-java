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

import reactor.core.publisher.Mono;

interface ResumedFramesCalculator {

  ResumedFramesCalculator ofClient = new ClientResumedFramesCalculator();
  ResumedFramesCalculator ofServer = new ServerResumedFramesCalculator();

  Mono<Long> calculate(ResumptionState local, ResumptionState remote);

  class ClientResumedFramesCalculator implements ResumedFramesCalculator {

    @Override
    public Mono<Long> calculate(ResumptionState clientState, ResumptionState serverState) {
      long serverImplied = serverState.impliedPosition();
      if (serverImplied >= clientState.position()) {
        return Mono.just(serverImplied);
      } else {
        return Mono.error(new ResumeStateException(clientState, serverState));
      }
    }
  }

  class ServerResumedFramesCalculator implements ResumedFramesCalculator {

    @Override
    public Mono<Long> calculate(ResumptionState serverState, ResumptionState clientState) {
      boolean clientStateValid = clientState.position() <= serverState.impliedPosition();
      boolean serverStateValid = serverState.position() <= clientState.impliedPosition();
      return clientStateValid && serverStateValid
          ? Mono.just(clientState.impliedPosition())
          : Mono.error(new ResumeStateException(serverState, clientState));
    }
  }
}
