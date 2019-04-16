package io.rsocket.test;

import java.lang.annotation.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * {@code @PerfTest} is used to signal that the annotated test class or method is performance test,
 * and is disabled unless enabled via setting the {@code TEST_PERF_ENABLED} environment variable to
 * {@code true}.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@EnabledIfEnvironmentVariable(named = "TEST_PERF_ENABLED", matches = "(?i)true")
@Test
public @interface PerfTest {}
