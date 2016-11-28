/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.perf;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.perf.util.ClientServerHolder;
import io.reactivesocket.util.PayloadImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class RequestStreamPerf extends AbstractReactiveSocketPerf {

    @Param({ "1", "100", "1000"})
    public int itemCount;

    @Setup(Level.Trial)
    public void setup(Blackhole bh) {
        _setup(bh);
    }

    @Benchmark
    public void requestStream() throws InterruptedException {
        Supplier<ReactiveSocket> socketSupplier = getSocketSupplier();
        requestStream(socketSupplier, () -> new PayloadImpl(ClientServerHolder.HELLO), 1, itemCount);
    }
}
