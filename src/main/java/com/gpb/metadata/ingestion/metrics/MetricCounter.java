package com.gpb.metadata.ingestion.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class MetricCounter {

    private final AtomicLong success = new AtomicLong();
    private final AtomicLong error = new AtomicLong();

    public void success() {
        success.incrementAndGet();
    }

    public void error() {
        error.incrementAndGet();
    }

    public long getSuccessCount() {
        return success.get();
    }

    public long getErrorCount() {
        return error.get();
    }
}
