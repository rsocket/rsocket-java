package io.reactivesocket.transport.tcp.client;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.transport.tcp.ReactiveSocketFrameCodec;
import io.reactivesocket.transport.tcp.ReactiveSocketLengthCodec;
import io.reactivesocket.transport.tcp.TcpDuplexConnection;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.Single;
import rx.Single.OnSubscribe;
import rx.SingleSubscriber;
import rx.Subscriber;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.reactivesocket.DefaultReactiveSocket.fromClientConnection;

public class TcpReactiveSocketConnector implements ReactiveSocketConnector<SocketAddress> {

    private final ConcurrentMap<SocketAddress, TcpClient<Frame, Frame>> socketFactories;
    private final ConnectionSetupPayload setupPayload;
    private final Consumer<Throwable> errorStream;
    private final Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory;

    private TcpReactiveSocketConnector(ConnectionSetupPayload setupPayload, Consumer<Throwable> errorStream,
                                       Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory) {
        this.setupPayload = setupPayload;
        this.errorStream = errorStream;
        this.clientFactory = clientFactory;
        socketFactories = new ConcurrentHashMap<>();
    }

    @Override
    public Publisher<ReactiveSocket> connect(SocketAddress address) {
        return _connect(socketFactories.computeIfAbsent(address, socketAddress -> {
            return clientFactory.apply(socketAddress)
                                .addChannelHandlerLast("length-codec", ReactiveSocketLengthCodec::new)
                                .addChannelHandlerLast("frame-codec", ReactiveSocketFrameCodec::new);
        }));
    }

    private Publisher<ReactiveSocket> _connect(TcpClient<Frame, Frame> client) {
        Single<ReactiveSocket> r = Single.create(new OnSubscribe<ReactiveSocket>() {
            @Override
            public void call(SingleSubscriber<? super ReactiveSocket> s) {
                client.createConnectionRequest()
                      .toSingle()
                      .unsafeSubscribe(new Subscriber<Connection<Frame, Frame>>() {
                          @Override
                          public void onCompleted() {
                              // Single contract does not allow complete without onNext and onNext here completes
                              // the outer subscriber
                          }

                          @Override
                          public void onError(Throwable e) {
                              s.onError(e);
                          }

                          @Override
                          public void onNext(Connection<Frame, Frame> c) {
                              TcpDuplexConnection dc = new TcpDuplexConnection(c);
                              ReactiveSocket rs = fromClientConnection(dc, setupPayload, errorStream);
                              rs.start(new Completable() {
                                  @Override
                                  public void success() {
                                      s.onSuccess(rs);
                                  }

                                  @Override
                                  public void error(Throwable e) {
                                      s.onError(e);
                                  }
                              });
                          }
                      });
            }
        });
        return RxReactiveStreams.toPublisher(r.toObservable());
    }

    @Override
    public String toString() {
        return "TcpReactiveSocketConnector";
    }

    public static TcpReactiveSocketConnector create(ConnectionSetupPayload setupPayload,
                                                    Consumer<Throwable> errorStream) {
        return new TcpReactiveSocketConnector(setupPayload, errorStream,
                                              socketAddress -> TcpClient.newClient(socketAddress));
    }

    public static TcpReactiveSocketConnector create(ConnectionSetupPayload setupPayload,
                                                    Consumer<Throwable> errorStream,
                                                    Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory) {
        return new TcpReactiveSocketConnector(setupPayload, errorStream, clientFactory);
    }
}
