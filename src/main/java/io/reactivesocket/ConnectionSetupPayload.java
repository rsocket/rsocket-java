/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket;

import java.nio.ByteBuffer;

/**
 * Exposed to server for determination of RequestHandler based on mime types and SETUP metadata/data
 */
public abstract class ConnectionSetupPayload implements Payload
{
    private final Frame frame;

    public ConnectionSetupPayload(final Frame frame)
    {
        Frame.ensureFrameType(FrameType.SETUP, frame);
        this.frame = frame;
    }

    public String metadataMimeType()
    {
        return Frame.Setup.metadataMimeType(frame);
    }

    public String dataMimeType()
    {
        return Frame.Setup.dataMimeType(frame);
    }

    public ByteBuffer getData()
    {
        return frame.getData();
    }

    public ByteBuffer getMetadata()
    {
        return frame.getMetadata();
    }
}
