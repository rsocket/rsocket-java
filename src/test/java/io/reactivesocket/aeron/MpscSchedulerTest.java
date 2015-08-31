package io.reactivesocket.aeron;

import org.junit.Test;
import rx.Observable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MpscSchedulerTest {
    @Test
    public void test() {
        Observable.range(1, 10, new MpscScheduler()).doOnNext(i -> {
            System.out.println("Thread " + Thread.currentThread() + " - the number is => " + i);
        }).toBlocking().last();
    }

    @Test
    public void test2() throws Exception {
        MpscScheduler mpscScheduler = new MpscScheduler();

        Observable.range(1, 245)
            .flatMap(j -> {
                return Observable.just(j).subscribeOn(mpscScheduler).doOnNext(i -> {
                    System.out.println("Thread " + Thread.currentThread() + " - the number is => " + i);
                });
            }).toBlocking().last();
        mpscScheduler.close();
    }


    @Test
    public void test3() throws Exception {
        MpscScheduler mpscScheduler = new MpscScheduler();

        Observable.just("hello").repeat(50, mpscScheduler).doOnNext(i -> {
            System.out.println("Thread " + Thread.currentThread() + " - the message is => " + i);
        }).toBlocking().last();
        mpscScheduler.close();
    }

    @Test
    public void test4()throws Exception {
        MpscScheduler mpscScheduler = new MpscScheduler();

        AtomicLong start = new AtomicLong(0);

        Observable
            .just("hello")
            .doOnNext(a -> start.set(System.currentTimeMillis()))
            .delay(1, TimeUnit.SECONDS, mpscScheduler)
            .doOnNext(a -> System.out.println("Delay == " + (System.currentTimeMillis() - start.get())))
            .doOnNext(i -> {
                System.out.println("Thread " + Thread.currentThread() + " - the message is => " + i);
            })
            .toBlocking().last();
        mpscScheduler.close();
    }


}