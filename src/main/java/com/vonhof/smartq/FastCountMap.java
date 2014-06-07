package com.vonhof.smartq;


public class FastCountMap {
    private final String[] keys;
    private final long[] values;

    public FastCountMap(int size) {
        this.keys = new String[size];
        this.values = new long[size];
    }

    public int indexOf(String search) {
        int i = 0;
        for(String key : keys) {

            if (key == null) {
                return -1;
            }

            if (search.equals(key)) {
                return i;
            }
            i++;
        }

        return -1;
    }

    private int ensureKey(String newKey) {
        int i = 0;

        for(String key : keys) {
            if (key == null) {
                keys[i] = newKey;
                return i;
            }

            if (newKey.equals(key)) {
                return i;
            }

            i++;
        }
        throw new ArrayIndexOutOfBoundsException("No more room for keys!");
    }

    public long increment(String key, long count) {
        int ix = ensureKey(key);
        values[ix] += count;
        return values[ix];
    }

    public long decrement(String key, long count) {
        int ix = ensureKey(key);
        values[ix] -= count;
        return values[ix];
    }

    public long get(String key, long defaultValue) {
        int ix = indexOf(key);
        if (ix < 0) {
            return defaultValue;
        }
        return values[ix];
    }

    public long set(String tag, long val) {
        int ix = ensureKey(tag);
        values[ix] = val;
        return values[ix];
    }
}
