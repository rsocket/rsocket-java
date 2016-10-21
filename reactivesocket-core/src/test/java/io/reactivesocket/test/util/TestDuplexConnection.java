/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.test.util;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.processors.UnicastProcessor;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.reactivex.subscribers.TestSubscriber;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An implementation of {@link DuplexConnection} that provides functionality to modify the behavior dynamically.
 */
public class TestDuplexConnection implements DuplexConnection {

    private static final Logger logger = LoggerFactory.getLogger(TestDuplexConnection.class);

    private final LinkedBlockingQueue<Frame> sent;
    private final UnicastProcessor<Frame> sentPublisher;
    private final UnicastProcessor<Frame> received;
    private final Processor<Void, Void> close;
    private final ConcurrentLinkedQueue<TestSubscriber<Frame>> sendSubscribers;
    private volatile double availability =1;
    private volatile int initialSendRequestN = Integer.MAX_VALUE;

    public TestDuplexConnection() {
        sent = new LinkedBlockingQueue<>();
        received = UnicastProcessor.create();
        sentPublisher = UnicastProcessor.create();
        sendSubscribers = new ConcurrentLinkedQueue<>();
        close = PublishProcessor.create();
    }

    @Override
    public Publisher<Void> send(Publisher<Frame> frames) {
        if (availability <= 0) {
            return Px.error(new IllegalStateException("ReactiveSocket not available. Availability: " + availability));
        }
        TestSubscriber<Frame> subscriber = TestSubscriber.create(initialSendRequestN);
        Flowable.fromPublisher(frames)
                .doOnNext(frame -> {
                sent.offer(frame);
                sentPublisher.onNext(frame);
            })
                .doOnError(throwable -> {
                logger.error("Error in send stream on test connection.", throwable);
            })
                .subscribe(subscriber);
        sendSubscribers.add(subscriber);
        return Px.empty();
    }

    @Override
    public Publisher<Frame> receive() {
        return received;
    }

    @Override
    public double availability() {
        return availability;
    }

    @Override
    public Publisher<Void> close() {
        return close;
    }

    @Override
    public Publisher<Void> onClose() {
        return close();
    }

    public Frame awaitSend() throws InterruptedException {
        return sent.take();
    }

    public void setAvailability(double availability) {
        this.availability = availability;
    }

    public Collection<Frame> getSent() {
        return sent;
    }

    public Publisher<Frame> getSentAsPublisher() {
        return sentPublisher;
    }

    public void addToReceivedBuffer(Frame... received) {
        for (Frame frame : received) {
            this.received.onNext(frame);
        }
    }

    public void clearSendReceiveBuffers() {
        sent.clear();
        sendSubscribers.clear();
    }

    public void setInitialSendRequestN(int initialSendRequestN) {
        this.initialSendRequestN = initialSendRequestN;
    }

    public Collection<TestSubscriber<Frame>> getSendSubscribers() {
        return sendSubscribers;
    }
}
