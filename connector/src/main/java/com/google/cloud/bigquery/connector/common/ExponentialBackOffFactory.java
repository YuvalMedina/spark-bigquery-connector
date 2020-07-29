package com.google.cloud.bigquery.connector.common;

import com.google.api.client.util.ExponentialBackOff;

import java.io.Serializable;
import java.util.OptionalDouble;
import java.util.OptionalInt;

public class ExponentialBackOffFactory implements Serializable {

    private final Integer maxElapsedTimeMillis;
    private final Integer initialIntervalMillis;
    private final Double multiplier;
    private final Integer maxIntervalMillis;

    public ExponentialBackOffFactory(OptionalInt maxElapsedTimeMillis,
                                     OptionalInt initialIntervalMillis,
                                     OptionalDouble multiplier,
                                     OptionalInt maxIntervalMillis) {
        this.maxElapsedTimeMillis = maxElapsedTimeMillis.isPresent() ? maxElapsedTimeMillis.getAsInt() : null;
        this.initialIntervalMillis = initialIntervalMillis.isPresent() ? initialIntervalMillis.getAsInt() : null;
        this.multiplier = multiplier.isPresent() ? multiplier.getAsDouble() : null;
        this.maxIntervalMillis = maxIntervalMillis.isPresent() ? maxIntervalMillis.getAsInt() : null;
    }

    public ExponentialBackOff createExponentialBackOff(){
        ExponentialBackOff.Builder builder = new ExponentialBackOff.Builder();
        if(maxElapsedTimeMillis != null) {
            builder.setMaxElapsedTimeMillis(maxElapsedTimeMillis);
        } if(initialIntervalMillis != null) {
            builder.setInitialIntervalMillis(initialIntervalMillis);
        } if(multiplier != null) {
            builder.setMultiplier(multiplier);
        } if(maxIntervalMillis != null) {
            builder.setMaxIntervalMillis(maxIntervalMillis);
        }
        return builder.build();
    }
}
