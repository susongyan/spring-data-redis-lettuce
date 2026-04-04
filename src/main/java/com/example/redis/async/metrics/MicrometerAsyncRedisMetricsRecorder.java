package com.example.redis.async.metrics;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

import com.example.redis.async.executor.CommandDescriptor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public final class MicrometerAsyncRedisMetricsRecorder implements AsyncRedisMetricsRecorder {

    private static final String METRIC_NAME = "async.redis.command";

    private final MeterRegistry meterRegistry;

    public MicrometerAsyncRedisMetricsRecorder(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    }

    @Override
    public Sample start(CommandDescriptor descriptor) {
        long startedAt = System.nanoTime();
        return new Sample() {
            @Override
            public void success() {
                record("success", "none");
            }

            @Override
            public void failure(Throwable error) {
                record("failure", error == null ? "unknown" : error.getClass().getSimpleName());
            }

            private void record(String outcome, String errorType) {
                Timer.builder(METRIC_NAME)
                        .tag("command", descriptor.command())
                        .tag("data_structure", descriptor.dataStructure().name().toLowerCase(Locale.ROOT))
                        .tag("outcome", outcome)
                        .tag("error", errorType)
                        .register(meterRegistry)
                        .record(Duration.ofNanos(System.nanoTime() - startedAt));
            }
        };
    }
}
