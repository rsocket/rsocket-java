/*
 * Copyright 2016 Facebook, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.tckdrivers.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivesocket.Payload;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Subscriber;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * This class parses through a marble diagram, but also implements a backpressure buffer so that the rate at
 * which producers add values can be much faster than the rate at which consumers consume values.
 * The backpressure buffer is the marble string
 */
public class ParseMarble {

    private String marble;
    private Subscriber<? super Payload> s;
    private boolean cancelled = false;
    private Map<String, Map<String, String>> argMap;
    private long numSent = 0;
    private long numRequested = 0;
    private int marbleIndex = 0;
    private CountDownLatch parseLatch;
    private CountDownLatch sendLatch;

    /**
     * This constructor is useful if one already has the entire marble diagram before hand, so add() does not need to
     * be called.
     * @param marble the whole marble diagram
     * @param s the subscriber
     */
    public ParseMarble(String marble, Subscriber<? super Payload> s) {
        this.s = s;
        this.marble = marble;
        if (marble.contains("&&")) {
            String[] temp = marble.split("&&");
            this.marble = temp[0];
            ObjectMapper mapper = new ObjectMapper();
            try {
                argMap = mapper.readValue(temp[1], new TypeReference<Map<String, Map<String, String>>>() {
                });
            } catch (Exception e) {
                System.out.println("couldn't convert argmap");
            }
        }
        parseLatch = new CountDownLatch(1);
        sendLatch = new CountDownLatch(1);
    }

    /**
     * This constructor is useful for channel, when the marble diagram will be build incrementally.
     * @param s the subscriber
     */
    public ParseMarble(Subscriber<? super Payload> s) {
        this.s = s;
        this.marble = "";
        parseLatch = new CountDownLatch(1);
        sendLatch = new CountDownLatch(1);
    }

    /**
     * This method is synchronized because we don't want two threads to try to add to the string at once.
     * Calling this method also unblocks the parseLatch, which allows non-emittable symbols to be sent. In other words,
     * it allows onNext and onComplete to be sent even if we've sent all the values we've been requested of.
     * @param m
     */
    public synchronized void add(String m) {
        System.out.println("adding " + m);
        this.marble += m;
        parseLatch.countDown();
    }

    /**
     * This method is synchronized because we only want to process one request at one time. Calling this method unblocks
     * the sendLatch as well as the parseLatch if we have more requests,
     * as it allows both emitted and non-emitted symbols to be sent,
     * @param n
     */
    public synchronized void request(long n) {
        System.out.println("requested" + n);
        numRequested += n;
        if (marble.length() > marbleIndex) {
            parseLatch.countDown();
        }
        if (n > 0) sendLatch.countDown();
    }

    /**
     * This function calls parse and executes the specified behavior in each line of commands
     */
    public void parse() {
        try {
            // if cancel has been called, don't do anything
            if (cancelled) return;
            while (true) {
                if (marbleIndex >= marble.length()) {
                    synchronized (parseLatch) {
                        if (parseLatch.getCount() == 0) parseLatch = new CountDownLatch(1);
                        parseLatch.await();
                    }
                    parseLatch = new CountDownLatch(1);
                }
                char c = marble.charAt(marbleIndex);
                switch (c) {
                    case '-':
                        // ignore the '-' characters
                        break;
                    case '|':
                        s.onComplete();
                        System.out.println("on complete sent");
                        break;
                    case '#':
                        s.onError(new Throwable());
                        break;
                    case '(':
                        // ignore groupings
                        break;
                    case ')':
                        // ignore groupings
                        break;
                    default:
                        if (numSent >= numRequested) {
                            synchronized (sendLatch) {
                                if (sendLatch.getCount() == 0) sendLatch = new CountDownLatch(1);
                                sendLatch.await();
                            }
                            sendLatch = new CountDownLatch(1);
                        }
                        if (argMap != null) {
                            // this is hacky, but we only expect one key and one value
                            Map<String, String> tempMap = argMap.get(c + "");
                            if (tempMap == null) {
                                s.onNext(new PayloadImpl(c + "", c + ""));
                                break;
                            }
                            List<String> key = new ArrayList<>(tempMap.keySet());
                            List<String> value = new ArrayList<>(tempMap.values());
                            s.onNext(new PayloadImpl(key.get(0), value.get(0)));
                        } else {
                            this.s.onNext(new PayloadImpl(c + "", c + ""));
                            System.out.println("DATA SENT");
                        }

                        numSent++;
                        break;
                }
                marbleIndex++;
            }
        } catch (InterruptedException e) {
            System.out.println("interrupted");
        }

    }

    /**
     * Since cancel is async, it just means that we will eventually, and rather quickly, stop emitting values.
     */
    public void cancel() {
        cancelled = true;
    }

}