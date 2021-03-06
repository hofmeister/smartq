package com.vonhof.smartq;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class EstimateMap<T> {
    private final Map<T,Long> totals = new ConcurrentHashMap<T, Long>();
    private final Map<T,Long> counts = new ConcurrentHashMap<T, Long>();

    private void ensureKey(T key) {
        if (!totals.containsKey(key)) {
            totals.put(key, 0L);
        }

        if (!counts.containsKey(key)) {
            counts.put(key, 0L);
        }
    }

    private void increment(T key, long duration) {
        ensureKey(key);

        duration = totals.get(key) + duration;
        totals.put(key, duration);

        long count = counts.get(key) + 1;
        counts.put(key, count);
    }

    public void add(T type, long duration) {
        increment(type, duration);
    }

    public void set(T type, long duration) {
        totals.put(type, duration);
        counts.put(type, 1L);
    }

    public long average(T type) {
        if (!totals.containsKey(type)) {
            return 0;
        }

        return totals.get(type) / counts.get(type);
    }
}
