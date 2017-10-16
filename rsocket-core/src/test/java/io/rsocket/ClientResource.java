package io.rsocket;

import static io.rsocket.FrameType.NEXT_COMPLETE;
import static io.rsocket.FrameType.REQUEST_RESPONSE;
import static io.rsocket.test.util.TestSubscriber.anyPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.verify;

import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class ClientResource {
  public final TestDuplexConnection connection;
  public final RSocketClient socket;
  public final ConcurrentLinkedQueue<Throwable> errors;

  public ClientResource() {
    connection = new TestDuplexConnection();
    errors = new ConcurrentLinkedQueue<>();
    socket =
        new RSocketClient(
            connection,
            errors::add,
            StreamIdSupplier.clientSupplier(),
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            4);
  }

  public int sendRequestResponse(Publisher<Payload> response) {
    Subscriber<Payload> sub = TestSubscriber.create();
    response.subscribe(sub);
    int streamId = getStreamIdForRequestType(REQUEST_RESPONSE);
    connection.addToReceivedBuffer(
        Frame.PayloadFrame.from(streamId, NEXT_COMPLETE, PayloadImpl.EMPTY));
    verify(sub).onNext(anyPayload());
    verify(sub).onComplete();
    return streamId;
  }

  public int getStreamIdForRequestType(FrameType expectedFrameType) {
    assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
    List<FrameType> framesFound = new ArrayList<>();
    for (Frame frame : connection.getSent()) {
      if (frame.getType() == expectedFrameType) {
        return frame.getStreamId();
      }
      framesFound.add(frame.getType());
    }
    throw new AssertionError(
        "No frames sent with frame type: " + expectedFrameType + ", frames found: " + framesFound);
  }
}
