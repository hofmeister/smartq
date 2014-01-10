package com.vonhof.smartq;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

public class Task<T> {

    private UUID id;
    private long estimatedDuration;
    private long actualDuration;
    private State state = State.PENDING;
    private long created = 0;
    private long started = 0;
    private long ended = 0;

    private int priority = 1;
    private int attempts = 0;

    private T data;
    private String referenceId;
    private TagSet tags = new TagSet();

    public Task() {
        this("none");
    }

    public Task withPriority(int priority) {
        setPriority(priority);
        return this;
    }

    public Task(String type, long estimatedDuration) {
        this(type);
        this.estimatedDuration = estimatedDuration;
    }

    public Task(String tag) {
        id = UUID.randomUUID();
        created = WatchProvider.currentTime();
        addTag(tag);
    }

    public void reset() {
        this.created = WatchProvider.currentTime();
        started = 0;
        ended = 0;
        state = State.PENDING;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
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

    public Task withId(UUID id) {
        this.setId(id);
        return this;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public TagSet getTags() {
        return tags;
    }

    public void setTags(Collection<String> tags) {
        this.tags = new TagSet(tags);
    }

    public void addTag(String tag) {
        tags.add(tag);
    }

    public Task withTag(String tag) {
        addTag(tag);
        return this;
    }

    public static enum State {
        PENDING,
        RUNNING,
        ERROR, DONE
    }

    public static class TagSet extends HashSet<String> {

        public TagSet() {
        }

        public TagSet(Collection<? extends String> strings) {
            super(strings);
        }

        public String first() {
            return iterator().next();
        }
    }
}
