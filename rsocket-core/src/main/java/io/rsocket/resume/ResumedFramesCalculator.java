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
