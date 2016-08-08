package io.reactivesocket.tckdrivers.common;

import io.reactivesocket.tckdrivers.server.JavaTCPServer;

public class ServerThread implements Runnable {
    private Thread t;
    private int port;
    private String serverfile;


    public ServerThread(int port, String serverfile) {
        t = new Thread(this);
        this.port = port;
        this.serverfile = serverfile;
    }


    @Override
    public void run() {
        new JavaTCPServer().run(serverfile, port);
    }

    public void start() {
        t.start();
    }
}