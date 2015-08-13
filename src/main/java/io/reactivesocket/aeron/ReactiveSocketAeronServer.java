package io.reactivesocket.aeron;

import io.reactivesocket.ReactiveSocketServerProtocol;
import io.reactivesocket.RequestHandler;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Image;

/**
 * Created by rroeser on 8/13/15.
 */
public class ReactiveSocketAeronServer {
    private final ReactiveSocketServerProtocol rsServerProtocol;

    private final Aeron aeron;

    private final int SERVER_STREAM_ID = 1;

    private final int port;

    public ReactiveSocketAeronServer(int port, RequestHandler requestHandler) {
        this.port = port;

        rsServerProtocol = ReactiveSocketServerProtocol.create(requestHandler);

        final Aeron.Context ctx = new Aeron.Context();
        ctx.newImageHandler(this::newImageHandler);

        aeron = Aeron.connect(ctx);

        aeron.addSubscription("udp://localhost:" + port, SERVER_STREAM_ID);
    }

    void newImageHandler(Image image, String channel, int streamId, int sessionId, long joiningPosition, String sourceIdentity) {
        if (SERVER_STREAM_ID == streamId) {

        } else {
            System.out.println("");
        }
    }
}
