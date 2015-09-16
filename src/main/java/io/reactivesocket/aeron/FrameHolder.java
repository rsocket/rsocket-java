package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import uk.co.real_logic.aeron.Publication;

public class FrameHolder {
    private Publication publication;
    private Frame frame;

    public FrameHolder(Frame frame, Publication publication) {
        this.frame = frame;
        this.publication = publication;
    }

    public Publication getPublication() {
        return publication;
    }

    public Frame getFrame() {
        return frame;
    }
}
