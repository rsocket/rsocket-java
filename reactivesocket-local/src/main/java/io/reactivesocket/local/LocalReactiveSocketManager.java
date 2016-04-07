package io.reactivesocket.local;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by rroeser on 4/2/16.
 */
class LocalReactiveSocketManager {
    private static final LocalReactiveSocketManager INSTANCE = new LocalReactiveSocketManager();

    private final ConcurrentHashMap<String, LocalServerDuplexConection> serverConnections;
    private final ConcurrentHashMap<String, LocalClientDuplexConnection> clientConnections;

    private LocalReactiveSocketManager() {
        serverConnections = new ConcurrentHashMap<>();
        clientConnections = new ConcurrentHashMap<>();
    }

    public static LocalReactiveSocketManager getInstance() {
        return INSTANCE;
    }

    public LocalClientDuplexConnection getClientConnection(String name) {
        return clientConnections.computeIfAbsent(name, LocalClientDuplexConnection::new);
    }

    public void removeClientConnection(String name) {
        clientConnections.remove(name);
    }

    public LocalServerDuplexConection getServerConnection(String name) {
        return serverConnections.computeIfAbsent(name, LocalServerDuplexConection::new);
    }

    public void removeServerDuplexConnection(String name) {
        serverConnections.remove(name);
    }

}
