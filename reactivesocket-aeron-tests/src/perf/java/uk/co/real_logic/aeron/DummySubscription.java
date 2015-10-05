package uk.co.real_logic.aeron;


import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.aeron.logbuffer.FileBlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;

import java.util.List;

public class DummySubscription extends Subscription {
    DummySubscription(ClientConductor conductor, String channel, int streamId, long registrationId) {
        super(conductor, channel, streamId, registrationId);
    }

    public DummySubscription() {
        super(null, null, 0, 0);
    }

    @Override
    public String channel() {
        return "";
    }

    @Override
    public int streamId() {
        return 0;
    }

    @Override
    public int poll(FragmentHandler fragmentHandler, int fragmentLimit) {
        return 0;
    }

    @Override
    public long blockPoll(BlockHandler blockHandler, int blockLengthLimit) {
        return 0;
    }

    @Override
    public long filePoll(FileBlockHandler fileBlockHandler, int blockLengthLimit) {
        return 0;
    }

    @Override
    public Image getImage(int sessionId) {
        return null;
    }

    @Override
    public List<Image> images() {
        return null;
    }

    @Override
    public void close() {

    }
}
