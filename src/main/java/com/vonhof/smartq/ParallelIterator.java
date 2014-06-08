package com.vonhof.smartq;

import java.util.Iterator;


public interface ParallelIterator<T> extends Iterator<T> {

    boolean canDoParallel();
    ParallelIterator[] getParallelIterators();

    void close();

    long size();
}
