# JMH Benchmarks

### Run All

```
./gradlew benchmarks
```

### Run Specific Class

```
./gradlew benchmarks '-Pjmh=.*FramePerf.*'
```

### Arguments

Optionally pass arguments for custom execution. Example:

```
./gradlew benchmarks '-Pjmh=-f 1 -tu s -bm thrpt -wi 5 -i 5 -r 1 .*FramePerf.*'
```

gives output like this:

```
# Warmup Iteration   1: 12699094.396 ops/s
# Warmup Iteration   2: 15101768.843 ops/s
# Warmup Iteration   3: 14991750.686 ops/s
# Warmup Iteration   4: 14819319.785 ops/s
# Warmup Iteration   5: 14856301.193 ops/s
Iteration   1: 14910334.272 ops/s
Iteration   2: 14954589.540 ops/s
Iteration   3: 15076277.267 ops/s
Iteration   4: 14833413.303 ops/s
Iteration   5: 14893188.328 ops/s


Result "encodeNextCompleteHello":
  14933560.542 ±(99.9%) 349800.467 ops/s [Average]
  (min, avg, max) = (14833413.303, 14933560.542, 15076277.267), stdev = 90842.071
  CI (99.9%): [14583760.075, 15283361.009] (assumes normal distribution)


# Run complete. Total time: 00:00:10

Benchmark                           Mode  Cnt         Score        Error  Units
FramePerf.encodeNextCompleteHello  thrpt    5  14933560.542 ± 349800.467  ops/s
```

To see all options:

```
./gradlew benchmarks '-Pjmh=-h'
```
