/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.aeron.server;

import io.aeron.driver.MediaDriver;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.Single;
import rx.functions.Func0;
import rx.observers.TestSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Ignore
public class ServerAeronManagerTest {
    @BeforeClass
    public static void init() {

        final MediaDriver.Context context = new MediaDriver.Context();
        context.dirsDeleteOnStart(true);
        final MediaDriver mediaDriver = MediaDriver.launch(context);

    }

    @Test(timeout = 2_000)
    public void testSubmitAction() throws Exception {
        ServerAeronManager instance = ServerAeronManager.getInstance();
        CountDownLatch latch = new CountDownLatch(1);
        instance.submitAction(() -> latch.countDown());
        latch.await();
    }

    @Test(timeout = 2_000)
    public void testSubmitTask() {
        ServerAeronManager instance = ServerAeronManager.getInstance();
        CountDownLatch latch = new CountDownLatch(1);
        Single<Long> longSingle = instance.submitTask(() ->
        {
            latch.countDown();
            return latch.getCount();
        });

        TestSubscriber testSubscriber = new TestSubscriber();
        longSingle.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
    }

    @Test(timeout = 2_0000)
    public void testSubmitTasks() {
        ServerAeronManager instance = ServerAeronManager.getInstance();
        int number = 1;
        List<Func0<Integer>> func0List = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            func0List.add(() -> number + 1);
        }

        TestSubscriber testSubscriber = new TestSubscriber();

        instance
            .submitTasks(Observable.from(func0List))
            .reduce((a,b) ->  a + b)
            .doOnError(t -> t.printStackTrace())
            .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
        testSubscriber.assertValue(200_000);
    }
}