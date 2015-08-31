package io.reactivesocket.aeron;

import rx.Scheduler;
import rx.Subscription;
import rx.exceptions.Exceptions;
import rx.functions.Action0;
import rx.internal.util.RxThreadFactory;
import rx.internal.util.unsafe.MpmcArrayQueue;
import rx.internal.util.unsafe.MpscLinkedQueue;
import rx.plugins.RxJavaPlugins;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 8/30/15.
 */
public class MpscScheduler extends Scheduler implements Closeable {
    private final MpscLinkedQueue<ActionWrapper>[] actionQueues;

    private AtomicLong count;

    private volatile boolean running = true;
    private long p1, p2, p3, p4, p5, p6, p7;

    private final int mask;
    private long p11, p12, p13, p14, p15, p16, p17;

    private ActionWrapperRecycler recycler;

    public MpscScheduler(int consumers, IdleStrategy idleStrategy) {
        actionQueues = new MpscLinkedQueue[consumers];
        recycler = new ActionWrapperRecycler();

        mask = consumers - 1;

        RxThreadFactory factory = new RxThreadFactory("MpscScheduler-");

        for (int i = 0; i < consumers; i++) {
            final MpscLinkedQueue<ActionWrapper> actionQueue = new MpscLinkedQueue<>();
            actionQueues[i] = actionQueue;

            final Thread thread = factory.newThread(() -> {
                while (running) {
                    ActionWrapper wrapper = null;
                    try {
                        while ((wrapper = actionQueue.poll()) == null) {
                            idleStrategy.idle(0);
                        }
                        wrapper.call();
                    } catch (Throwable t) {
                        Exceptions.throwIfFatal(t);
                        RxJavaPlugins.getInstance().getErrorHandler().handleError(t);
                    }
                    finally {
                        if (wrapper != null) {
                            recycler.release(wrapper);
                        }
                    }
                }
            });

            thread.start();
        }

        count = new AtomicLong(0);

    }

    public MpscScheduler() {
        this(Runtime.getRuntime().availableProcessors(), new NoOpIdleStrategy());
    }

    @Override
    public Worker createWorker() {
        return new MpscWorker((int) count.getAndIncrement() & mask);
    }

    @Override
    public void close() throws IOException {
        running = false;
    }

    class MpscWorker extends Worker {

        private int queue;

        private volatile boolean unsubscribe;

        private MpscLinkedQueue<ActionWrapper> actionQueue;

        public MpscWorker(int queue) {
            this.queue = queue;
            this.unsubscribe = false;
            this.actionQueue = actionQueues[queue];
        }

        @Override
        public Subscription schedule(Action0 action) {
            return schedule(action, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            ActionWrapper wrapper = recycler.lease();
            wrapper.set(queue, action, delayTime, unit);

            actionQueue.offer(wrapper);
            return wrapper;
        }

        @Override
        public void unsubscribe() {
            this.unsubscribe = true;
        }

        @Override
        public boolean isUnsubscribed() {
            return unsubscribe;
        }
    }

    private class ActionWrapperRecycler {
        MpmcArrayQueue<ActionWrapper> wrappers = new MpmcArrayQueue<>(128);

        public ActionWrapperRecycler() {
            for (int i = 0; i < 128; i++) {
                wrappers.offer(new ActionWrapper());
            }
        }

        public ActionWrapper lease() {
            ActionWrapper wrapper;
            while((wrapper = wrappers.poll()) == null) {}
            return wrapper;
        }

        public void release(ActionWrapper wrapper) {
            wrappers.offer(wrapper);
        }
    }

    private class ActionWrapper implements Subscription, Action0 {
        private Action0 wrapped;
        private long delayTime;
        private TimeUnit timeUnit;
        private volatile boolean unsubscribe;
        private int queue;
        private long start;

        // padding
        private long p1, p2, p3, p4, p5, p6, p7, p8, p9;
        private long p10, p11, p12, p13, p14, p15, p16, p17, p18, p19;
        private long p20, p21, p22, p23, p24, p25, p26, p27, p28, p29;
        private long p30, p31, p32, p33, p34, p35, p36, p37, p38, p39;
        private long p40, p41, p42;

        public void set(int queue, Action0 wrapped, long delayTime, TimeUnit timeUnit) {
            this.queue = queue;
            this.wrapped = wrapped;
            this.delayTime = delayTime;
            this.timeUnit = timeUnit;
            this.unsubscribe = false;
            this.start = System.nanoTime();
        }

        @Override
        public void call() {
            if (!unsubscribe) {
                if ((System.nanoTime() - start) >= timeUnit.toNanos(delayTime)) {
                    wrapped.call();
                } else {
                    actionQueues[queue].offer(this);
                }
            }
        }

        @Override
        public void unsubscribe() {
            this.unsubscribe = true;
        }

        @Override
        public boolean isUnsubscribed() {
            return unsubscribe;
        }

    }
}
