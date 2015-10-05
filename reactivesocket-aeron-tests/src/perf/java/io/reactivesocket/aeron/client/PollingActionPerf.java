package io.reactivesocket.aeron.client;


public class PollingActionPerf {
/*
    @State(Scope.Benchmark)
    public static class TestState {
        PollingAction pa;

        AtomicLong counter = new AtomicLong();

        @Setup
        public void init() {
            ClientAeronManager.SubscriptionGroup sg
                = new ClientAeronManager
                .SubscriptionGroup("foo",
                new Subscription[]{new DummySubscription()}, new Func1<Integer, ClientAeronManager.ThreadIdAwareFragmentHandler>() {
                @Override
                public ClientAeronManager.ThreadIdAwareFragmentHandler call(Integer integer) {
                    return new ClientAeronManager.ThreadIdAwareFragmentHandler(0) {
                        @Override
                        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                            counter.getAndIncrement();
                        }
                    };
                }
            });

            // 5 connections ....
            for (int i = 0; i < 5; i++) {
                sg.getClientActions().add(new ClientAeronManager.ClientAction(0) {
                    @Override
                    void call(int threadId) {
                        counter.getAndIncrement();
                    }

                    @Override
                    public void close() throws Exception {

                    }
                });
            }

            List<ClientAeronManager.SubscriptionGroup> group = new CopyOnWriteArrayList<>();
            group.add(sg);

            pa = new PollingAction(0, new ReentrantLock(), new ReentrantLock(), group);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(1)
    public void call1(TestState state) {
        state.pa.call();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(2)
    public void call2(TestState state) {
        state.pa.call();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(3)
    public void call3(TestState state) {
        state.pa.call();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(4)
    public void call4(TestState state) {
        state.pa.call();
    }
    */

}
