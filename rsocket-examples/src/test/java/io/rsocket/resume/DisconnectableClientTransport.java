package io.rsocket.resume;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;

class DisconnectableClientTransport implements ClientTransport {
  private final ClientTransport clientTransport;
  private final AtomicReference<DuplexConnection> curConnection = new AtomicReference<>();
  private long nextConnectPermitMillis;

  public DisconnectableClientTransport(ClientTransport clientTransport) {
    this.clientTransport = clientTransport;
  }

  @Override
  public Mono<DuplexConnection> connect(int mtu) {
    return Mono.defer(
        () ->
            now() < nextConnectPermitMillis
                ? Mono.error(new ClosedChannelException())
                : clientTransport
                .connect(mtu)
                .map(
                    c -> {
                      if (curConnection.compareAndSet(null, c)) {
                        return c;
                      } else {
                        throw new IllegalStateException(
                            "Transport supports at most 1 connection");
                      }
                    }));
  }

  public void disconnect() {
    disconnectFor(Duration.ZERO);
  }

  public void disconnectPermanently() {
    disconnectFor(Duration.ofDays(42));
  }

  public void disconnectFor(Duration cooldown) {
    DuplexConnection cur = curConnection.getAndSet(null);
    if (cur != null) {
      nextConnectPermitMillis = now() + cooldown.toMillis();
      cur.dispose();
    } else {
      throw new IllegalStateException("Trying to disconnect while not connected");
    }
  }

  private static long now() {
    return System.currentTimeMillis();
  }
}
