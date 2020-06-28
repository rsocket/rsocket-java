package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.rsocket.test.util.TestServerTransport;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class RSocketServerTest {

  @Test
  public void ensuresMaxFrameLengthCanNotBeLessThenMtu() {
    RSocketServer.create()
        .fragment(128)
        .bind(new TestServerTransport().withMaxFrameLength(64))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maximumTransmissionUnit[128] exceeds configured maxFrameLength[64]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPayloadSize() {
    RSocketServer.create()
        .maxInboundPayloadSize(128)
        .bind(new TestServerTransport().withMaxFrameLength(256))
        .as(StepVerifier::create)
        .expectErrorMessage("Configured maxFrameLength[256] exceeds maxPayloadSize[128]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPossibleFrameLength() {
    RSocketServer.create()
        .bind(new TestServerTransport().withMaxFrameLength(Integer.MAX_VALUE))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maxFrameLength["
                + Integer.MAX_VALUE
                + "] "
                + "exceeds maxFrameLength limit "
                + FRAME_LENGTH_MASK)
        .verify();
  }
}
