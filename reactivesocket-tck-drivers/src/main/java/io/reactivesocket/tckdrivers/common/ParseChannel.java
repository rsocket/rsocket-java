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

import io.reactivesocket.Payload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class is exclusively used to parse channel commands on both the client and the server
 */
public class ParseChannel {

    // colors for printing things out
    private final String ANSI_RESET = "\u001B[0m";
    private final String ANSI_RED = "\u001B[31m";
    private final String ANSI_GREEN = "\u001B[32m";

    private List<String> commands;
    private TestSubscriber<Payload> sub;
    private ParseMarble parseMarble;
    private String name = "";

    public ParseChannel(List<String> commands, TestSubscriber<Payload> sub, ParseMarble parseMarble) {
        this.commands = commands;
        this.sub = sub;
        this.parseMarble = parseMarble;
        ParseThread parseThread = new ParseThread(parseMarble);
        parseThread.start();
    }

    public ParseChannel(List<String> commands, TestSubscriber<Payload> sub, ParseMarble parseMarble, String name) {
        this.commands = commands;
        this.sub = sub;
        this.parseMarble = parseMarble;
        this.name = name;
        ParseThread parseThread = new ParseThread(parseMarble);
        parseThread.start();
    }


    public void parse() {
        for (String line : commands) {
            String[] args = line.split("%%");
            switch (args[0]) {
                case "respond":
                    handleResponse(args);
                    break;
                case "await":
                    switch (args[1]) {
                        case "terminal":
                            sub.awaitTerminalEvent();
                            break;
                        case "atLeast":
                            try {
                                sub.awaitAtLeast(Long.parseLong(args[3]), Long.parseLong(args[4]), TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                System.out.println("interrupted");
                            }
                            break;
                        case "no_events":
                            try {
                                sub.awaitNoEvents(Long.parseLong(args[3]));
                            } catch (InterruptedException e) {
                                System.out.println("interrupted");
                            }
                            break;
                    }
                    break;
                case "assert":
                    switch (args[1]) {
                        case "no_error":
                            sub.assertNoErrors();
                            break;
                        case "error":
                            sub.assertError(new Throwable());
                            break;
                        case "received":
                            handleReceived(args);
                            break;
                        case "received_n":
                            sub.assertValueCount(Integer.parseInt(args[3]));
                            break;
                        case "received_at_least":
                            sub.assertReceivedAtLeast(Integer.parseInt(args[3]));
                            break;
                        case "completed":
                            sub.assertComplete();
                            break;
                        case "no_completed":
                            sub.assertNotComplete();
                            break;
                        case "canceled":
                            sub.isCancelled();
                            break;
                    }
                    break;
                case "take":
                    handleTake(args);
                    break;
                case "request":
                    sub.request(Long.parseLong(args[1]));
                    System.out.println("requesting " + args[1]);
                    break;
                case "cancel":
                    sub.cancel();
                    break;
            }
        }
        if (name.equals("")) {
            name = "CHANNEL";
        }
        if (sub.hasPassed()) System.out.println(ANSI_GREEN + name + " PASSED" + ANSI_RESET);
        else System.out.println(ANSI_RED + name + " FAILED" + ANSI_RESET);
    }

    private void handleResponse(String[] args) {
        System.out.println("responding");
        AddThread addThread = new AddThread(args[1], parseMarble);
        addThread.start();
    }

    private void handleTake(String[] args) {
        RequestThread requestThread = new RequestThread(Long.parseLong(args[1]), parseMarble);
        requestThread.start();
        sub.cancel();
    }

    private void handleReceived(String[] args) {
        String[] values = args[3].split("&&");
        if (values.length == 1) {
            String[] temp = values[0].split(",");
            sub.assertValue(new Tuple<>(temp[0], temp[1]));
        } else if (values.length > 1) {
            List<Tuple<String, String>> assertList = new ArrayList<>();
            for (String v : values) {
                String[] vals = v.split(",");
                assertList.add(new Tuple<>(vals[0], vals[1]));
            }
            sub.assertValues(assertList);
        }
    }

}
