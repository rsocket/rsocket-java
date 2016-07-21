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
 * This class is rather straight forward for most interactions except for channel.
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
    private UUID id = UUID.randomUUID();

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

    // this is for channel, when the marble can be added incrementally
    public ParseMarble(Subscriber<? super Payload> s) {
        this.s = s;
        this.marble = "";
        parseLatch = new CountDownLatch(1);
        sendLatch = new CountDownLatch(1);
    }

    // adds stuff to the end of marble
    public synchronized void add(String m) {
        System.out.println("adding " + m);
        this.marble += m;
        parseLatch.countDown();
    }

    public synchronized void request(long n) {
        System.out.println("requested" + n);
        System.out.println(this.id);
        numRequested += n;
        if (marble.length() > marbleIndex) {
            parseLatch.countDown();
            sendLatch.countDown();
        }
    }

    // this parses the actual marble diagram and acts out the behavior
    // should be called upon triggering a handler
    public void parse() {
        try {
            // if cancel has been called, don't do anything
            if (cancelled) return;
            String buffer = "";
            boolean grouped = false;
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
                        if (grouped) buffer += c;
                        else try {
                            Thread.sleep(10);
                        } catch (Exception e) {
                            System.out.println("Interrupted");
                        }
                        break;
                    case '|':
                        if (grouped) buffer += c;
                        else s.onComplete();
                        System.out.println("on complete sent");
                        break;
                    case '#':
                        if (grouped) buffer += c;
                        else s.onError(new Throwable("error"));
                        break;
                    case '(':
                        // ignore groupings for now
                        break;
                    case ')':
                        // ignore groupings for now
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

    // cancel says that values will eventually stop being sent, which means we can wait till we've processed the initial
    // batch before sending
    public void cancel() {
        cancelled = true;
    }

}