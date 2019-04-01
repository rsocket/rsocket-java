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

  Mono<Long> calculate(long localPos, long localImpliedPos, long remotePos, long remoteImpliedPos);

  class ClientResumedFramesCalculator implements ResumedFramesCalculator {

    /*ResumptionState clientState, ResumptionState serverState*/
    @Override
    public Mono<Long> calculate(
        long clientPos, long clientImpliedPos, long serverPos, long serverImpliedPos) {
      if (serverImpliedPos >= clientPos) {
        return Mono.just(serverImpliedPos);
      } else {
        return Mono.error(
            new ResumeStateException(
                clientPos, clientImpliedPos,
                serverPos, serverImpliedPos));
      }
    }
  }

  class ServerResumedFramesCalculator implements ResumedFramesCalculator {

    /*ResumptionState serverState, ResumptionState clientState*/
    @Override
    public Mono<Long> calculate(
        long serverPos, long serverImpliedPos, long clientPos, long clientImpliedPos) {
      boolean clientStateValid = clientPos <= serverImpliedPos;
      boolean serverStateValid = serverPos <= clientImpliedPos;
      return clientStateValid && serverStateValid
          ? Mono.just(clientImpliedPos)
          : Mono.error(
              new ResumeStateException(
                  serverPos, serverImpliedPos,
                  clientPos, clientImpliedPos));
    }
  }
}
