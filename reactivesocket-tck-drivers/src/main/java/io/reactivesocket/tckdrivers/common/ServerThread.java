package io.reactivesocket.tckdrivers.common;

import io.reactivesocket.tckdrivers.server.JavaTCPServer;

public class ServerThread implements Runnable {
    private Thread t;
    private int port;
    private String serverfile;
    private JavaTCPServer server;

    public ServerThread(int port, String serverfile) {
        t = new Thread(this);
        this.port = port;
        this.serverfile = serverfile;
        server = new JavaTCPServer();
    }


    @Override
    public void run() {
        server.run(serverfile, port);
    }

    public void start() {
        t.start();
    }

    public void awaitStart() {
        server.awaitStart();
    }

}