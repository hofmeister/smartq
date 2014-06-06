package com.vonhof.smartq;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class CombinedIterator<T> implements Iterator<T> {
    private List<Iterator<T>> iterators = new LinkedList<Iterator<T>>();

    public CombinedIterator(Iterator<T> ... its ) {
        for(Iterator<T> it : its) {
            iterators.add(it);
        }
    }

    @Override
    public boolean hasNext() {
        for(Iterator<T> it : iterators) {
            if (it.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public T next() {
        for(Iterator<T> it : iterators) {
            if (it.hasNext()) {
                return it.next();
            }
        }
        return null;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Method not supported");
    }
}
