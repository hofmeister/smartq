package com.vonhof.smartq;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CountMap<T>  {

    private final Map<T,Long> map = new HashMap<T, Long>();


    private void ensureKey(T key) {
        if (!map.containsKey(key)) {
            map.put(key, 0L);
        }
    }

    public synchronized long increment(T key, long count) {
        ensureKey(key);

        count = map.get(key) + count;
        map.put(key, count);

        return count;
    }

    public synchronized long decrement(T key, long count) {
        ensureKey(key);

        count = map.get(key) - count;
        map.put(key, count);

        return count;
    }

    public synchronized void set(T key, long count) {
        map.put(key, count);
    }

    public synchronized long get(T key) {

        if (!map.containsKey(key)) {
            return 0L;
        }

        return map.get(key);
    }

    public long total() {
        long total = 0;
        for(Long count: map.values()) {
            total += count;
        }
        return total;
    }

    public Set<Map.Entry<T,Long>> entrySet() {
        return Collections.unmodifiableSet(map.entrySet());
    }

    /**
     * Set value if larger than the current value
     * @param t
     * @param count
     */
    public synchronized void setIfBigger(T t, long count) {
        if (get(t) < count) {
            set(t,count);
        }
    }

    /**
     * Set value if smaller than the current value
     * @param t
     * @param count
     */
    public synchronized void setIfSmaller(T t, long count) {
        if (get(t) > count) {
            set(t,count);
        }
    }

    public boolean contains(T type) {
        return map.containsKey(type);
    }

    public Set<T> keySet() {
        return map.keySet();
    }
}
