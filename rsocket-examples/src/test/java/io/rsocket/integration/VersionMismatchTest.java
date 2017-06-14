package io.rsocket.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.rsocket.*;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.VersionFlyweight;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class VersionMismatchTest {
  private NettyContextCloseable server;

  @After
  public void cleanup() {
    server.close().block();
  }

  @Test(timeout = 5_000L)
  public void testBadVersion() throws InterruptedException {
    TcpServerTransport serverTransport = TcpServerTransport.create(0);
    DuplexConnectionInterceptor x =
        new DuplexConnectionInterceptor() {
          @Override
          public DuplexConnection apply(Type type, DuplexConnection duplexConnection) {
            if (type == Type.SOURCE) {
              return new DuplexConnectionDecorator(duplexConnection) {
                @Override
                public Flux<Frame> receive() {
                  return delegate
                      .receive()
                      .map(
                          frame -> {
                            if (frame.getType() == FrameType.SETUP) {
                              int badVersion = VersionFlyweight.encode(99, 34);
                              frame
                                  .content()
                                  .setInt(FrameHeaderFlyweight.FRAME_HEADER_LENGTH, badVersion);
                            }
                            return frame;
                          });
                }
              };
            }

            return duplexConnection;
          }
        };

    RSocketFactory.Start<Closeable> transport =
        RSocketFactory.receive()
            .addConnectionPlugin(x)
            .acceptor(
                (setup, sendingSocket) -> Mono.just(new RSocketProxy(new AbstractRSocket() {})))
            .transport(serverTransport);
    server = transport.start().cast(NettyContextCloseable.class).block();

    RSocket client =
        RSocketFactory.connect()
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();
    Boolean hasElements =
        client.requestStream(new PayloadImpl("REQUEST", "META")).log().hasElements().block();

    assertFalse(hasElements);
  }
}
