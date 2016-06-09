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
package io.reactivesocket.internal.frame;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;


public class PayloadReassembler implements Subscriber<Frame>
{
    private final Subscriber<? super Payload> child;
    private final Int2ObjectHashMap<PayloadBuilder> payloadByStreamId = new Int2ObjectHashMap<>();

    private PayloadReassembler(final Subscriber<? super Payload> child)
    {
        this.child = child;
    }

    public static PayloadReassembler with(final Subscriber<? super Payload> child)
    {
        return new PayloadReassembler(child);
    }

    public void resetStream(final int streamId)
    {
        payloadByStreamId.remove(streamId);
    }

    public void onSubscribe(Subscription s)
    {
        // reset
    }

    public void onNext(Frame frame)
    {
        // if frame has no F bit and no waiting payload, then simply pass on
        final int streamId = frame.getStreamId();
        PayloadBuilder payloadBuilder = payloadByStreamId.get(streamId);

        if (FrameHeaderFlyweight.FLAGS_RESPONSE_F != (frame.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F))
        {
            Payload deliveryPayload = frame;

            // terminal frame
            if (null != payloadBuilder)
            {
                payloadBuilder.append(frame);
                deliveryPayload = payloadBuilder.payload();
                payloadByStreamId.remove(streamId);
            }

            child.onNext(deliveryPayload);
        }
        else
        {
            if (null == payloadBuilder)
            {
                payloadBuilder = new PayloadBuilder();
                payloadByStreamId.put(streamId, payloadBuilder);
            }

            payloadBuilder.append(frame);
        }
    }

    public void onError(Throwable t)
    {
        // reset and pass through
    }

    public void onComplete()
    {
        // reset and pass through
    }
}
