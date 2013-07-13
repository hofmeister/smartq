package com.vonhof.smartq;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Date;
import java.util.UUID;

public class Task<T extends Serializable> implements Serializable {

    private UUID id;
    private String type;
    private long estimatedDuration;
    private long actualDuration;
    private State state = State.PENDING;
    private long started = 0;
    private long ended = 0;

    private T data;

    public Task() {
        this("");
    }

    public Task(String type, long estimatedDuration) {
        this(type);
        this.estimatedDuration = estimatedDuration;
    }

    public Task(String type) {
        this.id = UUID.randomUUID();
        this.type = type;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public UUID getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public long getStarted() {
        return started;
    }

    public long getEnded() {
        return ended;
    }

    public void setEnded(long ended) {
        this.ended = ended;
    }

    public void setStarted(long ended) {
        this.ended = ended;
    }

    public void setState(State state) {
        this.state = state;
    }

    @JsonIgnore
    public boolean isRunning() {
        return state.equals(State.RUNNING);
    }

    public String getType() {
        return type;
    }

    @JsonIgnore
    public long getEstimatedTimeLeft() {
        switch (state) {
            case PENDING:
                return getEstimatedDuration();
            case RUNNING:
                long currentDuration = WatchProvider.currentTime() - started;
                if (currentDuration > getEstimatedDuration()) {
                    //Has exceeded the estimated time - we don't know how much might be left.
                    return 0;
                }
                return getEstimatedDuration() - currentDuration;
        }

        return 0;
    }

    public long getEstimatedDuration() {
        return estimatedDuration;
    }

    public void setEstimatedDuration(long estimatedDuration) {
        this.estimatedDuration = estimatedDuration;
    }

    public long getActualDuration() {
        return actualDuration;
    }

    public void setActualDuration(long actualDuration) {
        this.actualDuration = actualDuration;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }


    public static enum State {
        PENDING,
        RUNNING,
        DONE
    }
}
