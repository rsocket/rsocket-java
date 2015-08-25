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
	public static ConnectionSetupPayload create(String metadataMimeType, String dataMimeType) {
    	return new ConnectionSetupPayload() {
    	    public String metadataMimeType()
    	    {
    	        return metadataMimeType;
    	    }

    	    public String dataMimeType()
    	    {
    	        return dataMimeType;
    	    }

    	    public ByteBuffer getData()
    	    {
    	        return Frame.NULL_BYTEBUFFER;
    	    }

    	    public ByteBuffer getMetadata()
    	    {
    	    	return Frame.NULL_BYTEBUFFER;
    	    }
    	};
	}
	
	public static ConnectionSetupPayload create(String metadataMimeType, String dataMimeType, Payload payload) {
    	return new ConnectionSetupPayload() {
    	    public String metadataMimeType()
    	    {
    	        return metadataMimeType;
    	    }

    	    public String dataMimeType()
    	    {
    	        return dataMimeType;
    	    }

    	    public ByteBuffer getData()
    	    {
    	        return payload.getData();
    	    }

    	    public ByteBuffer getMetadata()
    	    {
    	    	return payload.getMetadata();
    	    }
    	};
	}
	
    public static ConnectionSetupPayload create(final Frame setupFrame)
    {
    	Frame.ensureFrameType(FrameType.SETUP, setupFrame);
    	return new ConnectionSetupPayload() {
    	    public String metadataMimeType()
    	    {
    	        return Frame.Setup.metadataMimeType(setupFrame);
    	    }

    	    public String dataMimeType()
    	    {
    	        return Frame.Setup.dataMimeType(setupFrame);
    	    }

    	    public ByteBuffer getData()
    	    {
    	        return setupFrame.getData();
    	    }

    	    public ByteBuffer getMetadata()
    	    {
    	        return setupFrame.getMetadata();
    	    }
    	};
    }

    public abstract String metadataMimeType();

    public abstract String dataMimeType();

    public abstract ByteBuffer getData();

    public abstract ByteBuffer getMetadata();
}
