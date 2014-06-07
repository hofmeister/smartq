package com.vonhof.smartq;


import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class FastCountMap {
    private final int[] keys;
    private final long[] values;

    public FastCountMap(Collection<String> keys, long defaultValue) {
        this.keys = new int[keys.size()];
        this.values = new long[keys.size()];
        Iterator<String> it = keys.iterator();
        int i = 0;
        while(it.hasNext()) {
            this.keys[i] = it.next().hashCode();
            i++;
        }
        Arrays.sort(this.keys);
        Arrays.fill(values,defaultValue);
    }

    public int indexOf(String search) {
        return Arrays.binarySearch(keys, search.hashCode());
    }

    public long increment(String key, long count) {
        int ix = indexOf(key);
        values[ix] += count;
        return values[ix];
    }

    public long decrement(String key, long count) {
        int ix = indexOf(key);
        values[ix] -= count;
        return values[ix];
    }

    public long get(String key) {
        int ix = indexOf(key);
        return values[ix];
    }

    public long set(String tag, long val) {
        int ix = indexOf(tag);
        values[ix] = val;
        return values[ix];
    }
}
