package com.vonhof.smartq;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CountMap<T>  {

    private final Map<T,Long> map = new HashMap<T, Long>();
    private long total;

    public CountMap() {
    }

    public CountMap(Map<T, Integer> other) {
        putAll(other);
    }

    public void putAll(Map<T, Integer> other) {
        for(Map.Entry<T,Integer> entry : other.entrySet()) {
            map.put(entry.getKey(), entry.getValue().longValue());
            total += entry.getValue().longValue();
        }
    }

    private void ensureKey(T key) {
        if (!map.containsKey(key)) {
            map.put(key, 0L);
        }
    }

    public long increment(T key, long count) {
        ensureKey(key);

        count = map.get(key) + count;
        map.put(key, count);
        total += count;

        return count;
    }

    public long decrement(T key, long count) {
        ensureKey(key);

        count = map.get(key) - count;
        map.put(key, count);
        total -= count;

        return count;
    }

    public void set(T key, long count) {
        map.put(key, count);
    }

    public long get(T key) {
        Long val = map.get(key);
        if (val == null) {
            return 0L;
        }
        return val;
    }

    public long total() {
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

    public void clear() {
        map.clear();
    }

    public void remove(T referenceId) {
        map.remove(referenceId);
    }

    public Map<T, Long> toMap() {
        return new HashMap<>(map);
    }
}
