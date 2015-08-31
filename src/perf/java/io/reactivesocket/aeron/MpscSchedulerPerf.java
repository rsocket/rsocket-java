package io.reactivesocket.aeron;

import io.reactivesocket.aeron.jmh.InputWithIncrementingInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import rx.Observable;
import rx.schedulers.Schedulers;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 8/30/15.
 */
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MpscSchedulerPerf {
    @State(Scope.Thread)
    public static class Input extends InputWithIncrementingInteger {

        @Param({ "100", "1000", "1000000" })
        public int size;

        @Override
        public int getSize() {
            return size;
        }
    }

    private static final MpscScheduler mpsc = new MpscScheduler(3, new NoOpIdleStrategy());

    @Benchmark
    public void observableConsumption(Input input) throws InterruptedException {
        input.firehose.subscribeOn(mpsc).subscribe(input.observer);
    }


    @Benchmark
    public void observableConsumption2(Input input) throws InterruptedException {
        Observable
            .just(1)
            .flatMap(i -> {
                return input.firehose.subscribeOn(mpsc);
            })
            .subscribe(input.observer);
    }

    //@Benchmark
    public void observableConsumption3(Input input) throws InterruptedException {
        input.firehose.subscribeOn(Schedulers.computation()).subscribe(input.observer);
    }

}
