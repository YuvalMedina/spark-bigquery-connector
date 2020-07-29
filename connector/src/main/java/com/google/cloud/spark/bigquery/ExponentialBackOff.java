package com.google.cloud.spark.bigquery;

import com.google.api.client.util.BackOff;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

public class ExponentialBackOff /*implements BackOff*/ {/*

    private Duration currentCumulativeBackOff = null;
    private int currentRetry = 0;
    private double DEFAULT_RANDOMIZATION_FACTOR = 0.5;

    @Override
    public void reset() throws IOException {
        currentRetry = 0;
        currentCumulativeBackOff = Duration.ZERO;
    }

    @Override
    public long nextBackOffMillis() throws IOException {
        if (currentRetry > MAX_RETRIES) {
            return BackOff.STOP;
        }
        if (currentCumulativeBackOff.compareTo(MAX_CUMULATIVE_BACKOFF) >= 0) {
            return BackOff.STOP;
        }

        long currentIntervalMillis = Math.min(
                backoffConfig.initialBackoff.toMillis * Math.pow(backoffConfig.exponent, currentRetry),
                backoffConfig.maxBackoff.toMillis);
        double randomOffset = (Math.random() * 2 - 1) *
                DEFAULT_RANDOMIZATION_FACTOR *
                currentIntervalMillis;
        long nextBackoffMillis = currentIntervalMillis + randomOffset.round;

        // Cap to limit on cumulative backoff
        long remainingCumulative = backoffConfig.maxCumulativeBackoff.minus(currentCumulativeBackoff);
        nextBackoffMillis = Math.min(nextBackoffMillis, remainingCumulative.toMillis);

        // Update state and return backoff.
        Duration currentCumulativeBackoff = currentCumulativeBackoff.plus(
                Duration.of(nextBackoffMillis, (TemporalUnit) TimeUnit.MILLISECONDS));
        currentRetry += 1;
        return nextBackoffMillis;
    }*/
}
