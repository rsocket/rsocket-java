package io.reactivesocket.integration;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class IntegrationTest2 {
    // This will throw as it completes without any onNext
    @Test(timeout = 2_000L, expected = NoSuchElementException.class)
    public void testCompleteWithoutNext() throws InterruptedException {
        ReactiveSocket requestHandler = mock(ReactiveSocket.class);

        when(requestHandler.requestStream(any()))
                .thenReturn(Flowable.empty());

        TransportServer.StartedServer server = ReactiveSocketServer.create(TcpTransportServer.create())
                .start((setup, sendingSocket) -> {
                    Flowable.fromPublisher(sendingSocket.onClose())
                            .subscribe();

                    return new DisabledLeaseAcceptingSocket(requestHandler);
                });
        ReactiveSocket client = Single.fromPublisher(ReactiveSocketClient.create(TcpTransportClient.create(server.getServerAddress()),
                SetupProvider.keepAlive(KeepAliveProvider.never())
                        .disableLease())
                .connect())
                .blockingGet();

        Flowable.fromPublisher(client.requestStream(new PayloadImpl("REQUEST", "META")))
                .blockingFirst();

        Thread.sleep(100);
        verify(requestHandler).requestStream(new PayloadImpl("REQUEST", "META"));
    }

    @Test(timeout = 2_000L)
    public void testSingle() throws InterruptedException {
        ReactiveSocket requestHandler = mock(ReactiveSocket.class);

        when(requestHandler.requestStream(any()))
                .thenReturn(Flowable.just(new PayloadImpl("RESPONSE", "METADATA")));

        TransportServer.StartedServer server = ReactiveSocketServer.create(TcpTransportServer.create())
                .start((setup, sendingSocket) -> {
                    Flowable.fromPublisher(sendingSocket.onClose())
                            .subscribe();

                    return new DisabledLeaseAcceptingSocket(requestHandler);
                });
        ReactiveSocket client = Single.fromPublisher(ReactiveSocketClient.create(TcpTransportClient.create(server.getServerAddress()),
                SetupProvider.keepAlive(KeepAliveProvider.never())
                        .disableLease())
                .connect())
                .blockingGet();

        Flowable.fromPublisher(client.requestStream(new PayloadImpl("REQUEST", "META")))
                .blockingSingle();

        verify(requestHandler).requestStream(any());
    }

    @Test()
    public void testZeroPayload() throws InterruptedException {
        ReactiveSocket requestHandler = mock(ReactiveSocket.class);

        when(requestHandler.requestStream(any()))
                .thenReturn(Flowable.just(PayloadImpl.EMPTY));

        TransportServer.StartedServer server = ReactiveSocketServer.create(TcpTransportServer.create())
                .start((setup, sendingSocket) -> {
                    Flowable.fromPublisher(sendingSocket.onClose())
                            .subscribe();

                    return new DisabledLeaseAcceptingSocket(requestHandler);
                });
        ReactiveSocket client = Single.fromPublisher(ReactiveSocketClient.create(TcpTransportClient.create(server.getServerAddress()),
                SetupProvider.keepAlive(KeepAliveProvider.never())
                        .disableLease())
                .connect())
                .blockingGet();

        int dataSize = Flowable.fromPublisher(client.requestStream(new PayloadImpl("REQUEST", "META")))
                .map(p -> p.getData().remaining())
                .blockingSingle();

        verify(requestHandler).requestStream(any());
        Assert.assertEquals(0, dataSize);
    }
}
