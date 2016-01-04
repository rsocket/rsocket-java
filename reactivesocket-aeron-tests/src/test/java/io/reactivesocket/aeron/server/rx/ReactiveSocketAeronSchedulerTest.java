package io.reactivesocket.aeron.server.rx;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.TimeUnit;


@Ignore
public class ReactiveSocketAeronSchedulerTest {
    @BeforeClass
    public static void init() {

        final MediaDriver.Context context = new MediaDriver.Context();
        context.dirsDeleteOnStart(true);
        final MediaDriver mediaDriver = MediaDriver.launch(context);

    }

    @Test
    public void test() {
        TestSubscriber testSubscriber = new TestSubscriber();

        Observable
                .range(0, 10)
                .subscribeOn(ReactiveSocketAeronScheduler.getInstance())
                .doOnNext(i -> {
                    String name = Thread.currentThread().getName();
                    Assert.assertTrue(name.contains("reactive-socket-aeron-server"));
                    System.out.println(name + " - " + i);
                })
                .observeOn(Schedulers.computation())
                .doOnNext(i -> {
                    String name = Thread.currentThread().getName();
                    Assert.assertTrue(name.contains("RxComputationThreadPool"));
                    System.out.println(name + " - " + i);
                })
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertValueCount(10);
    }

    @Test
    public void testWithFlatMap() {
        TestSubscriber testSubscriber = new TestSubscriber();

        Observable
                .range(0, 10)
                .flatMap(i ->
                    Observable
                        .just(i)
                        .subscribeOn(ReactiveSocketAeronScheduler.getInstance())
                )
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertValueCount(10);

    }

    @Test
    public void testMovingOnAndOffAndOnThePollingThread() {
        TestSubscriber testSubscriber = new TestSubscriber();
        Observable
                .range(0, 10)
                .subscribeOn(ReactiveSocketAeronScheduler.getInstance())
                .doOnNext(i -> {
                    String name = Thread.currentThread().getName();
                    Assert.assertTrue(name.contains("reactive-socket-aeron-server"));
                    System.out.println(name + " - " + i);
                })
                .flatMap(i ->
                        Observable
                                .just(i)
                                .subscribeOn(Schedulers.computation())
                                .doOnNext(j -> {
                                    String name = Thread.currentThread().getName();
                                    Assert.assertTrue(name.contains("RxComputationThreadPool"));
                                    System.out.println(name + " - " + i);
                                })
                )
                .observeOn(ReactiveSocketAeronScheduler.getInstance())
                .doOnNext(i -> {
                    String name = Thread.currentThread().getName();
                    Assert.assertTrue(name.contains("reactive-socket-aeron-server"));
                    System.out.println(name + " - " + i);
                })
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertValueCount(10);
    }

}