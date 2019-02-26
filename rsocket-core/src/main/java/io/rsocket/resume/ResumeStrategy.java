package io.rsocket.resume;

import java.util.function.BiFunction;
import org.reactivestreams.Publisher;

@FunctionalInterface
public interface ResumeStrategy extends BiFunction<ClientResume, Throwable, Publisher<?>> {}
