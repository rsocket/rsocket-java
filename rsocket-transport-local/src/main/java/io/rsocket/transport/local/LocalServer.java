package io.rsocket.transport.local;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.TransportServer;
import reactor.core.publisher.MonoProcessor;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class LocalServer implements TransportServer {
    private static final ConcurrentMap<String, StartServerImpl> registry = new ConcurrentHashMap<>();

    static StartServerImpl findServer(String name) {
        return registry.get(name);
    }

    private final String name;

    private LocalServer(String name) {
        this.name = name;
    }

    public static LocalServer create(String name) {
        return new LocalServer(name);
    }

    @Override
    public StartedServer start(ConnectionAcceptor acceptor) {
        StartServerImpl startedServer = new StartServerImpl(name, acceptor);
        if (registry.putIfAbsent(name, startedServer) != null) {
            throw new IllegalStateException("name already registered: " + name);
        }
        return startedServer;
    }

    static class StartServerImpl implements StartedServer, Consumer<DuplexConnection> {
        private final LocalSocketAddress address;
        private final ConnectionAcceptor acceptor;
        private final MonoProcessor<Void> closeNotifier = MonoProcessor.create();

        public StartServerImpl(String name, ConnectionAcceptor acceptor) {
            this.address = new LocalSocketAddress(name);
            this.acceptor = acceptor;
        }

        @Override
        public void accept(DuplexConnection duplexConnection) {
            acceptor.apply(duplexConnection).subscribe();
        }

        @Override
        public SocketAddress getServerAddress() {
            return address;
        }

        @Override
        public int getServerPort() {
            return 0;
        }

        @Override
        public void awaitShutdown() {
            closeNotifier.block();
        }

        @Override
        public void awaitShutdown(long duration, TimeUnit durationUnit) {
            closeNotifier.blockMillis(TimeUnit.MILLISECONDS.convert(duration, durationUnit));
        }

        @Override
        public void shutdown() {
            if (!registry.remove(address.getName(), this)) {
              throw new AssertionError();
            }
            closeNotifier.onComplete();
        }
    }
}
