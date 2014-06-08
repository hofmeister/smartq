package com.vonhof.smartq;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Task<T> {

    private UUID id;
    private State state = State.PENDING;
    private long created = 0;
    private long started = 0;
    private long ended = 0;

    private int priority = 1;
    private int attempts = 0;

    private T data;
    private String referenceId;
    private Map<String,Integer> tags = new HashMap<String, Integer>();
    private String type;

    public Task() {
        this("none");
    }

    public Task(Task<T> task) {
        this.id = task.id;
        this.state = task.state;
        this.created = task.created;
        this.started = task.started;
        this.ended = task.ended;
        this.priority = task.priority;
        this.attempts = task.attempts;
        this.data = task.data;
        this.referenceId = task.referenceId;
        this.tags = task.tags;
        this.type = task.type;
    }

    public Task withPriority(int priority) {
        setPriority(priority);
        return this;
    }

    public Task(String tag) {
        id = UUID.randomUUID();
        created = WatchProvider.currentTime();
        addTag(tag);
        this.type = tag;
    }

    @JsonIgnore
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

    public void setStarted(long started) {
        this.started = started;
    }

    public void setState(State state) {
        this.state = state;
    }

    @JsonIgnore
    public boolean isRunning() {
        return state.equals(State.RUNNING);
    }

    public long getActualDuration() {
        return ended > 0 ? ended - started : WatchProvider.currentTime() - started;
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

    public Map<String, Integer> getTags() {
        return tags;
    }

    public void setTags(Map<String, Integer> tags) {
        this.tags = tags;
    }

    public void addTag(String tag) {
        addRateLimit(tag, -1);
    }

    public Task withTag(String tag) {
        addTag(tag);
        return this;
    }

    public void setType(String type) {
        this.type = type;
        if (!tags.containsKey(type)) {
            addTag(type);
        }
    }

    public String getType() {
        return type;
    }

    public void addRateLimit(String tag, int maxConcurrency) {
        tags.put(tag, maxConcurrency);
    }

    @JsonIgnore
    public Set<String> getTagSet() {
        return tags.keySet();
    }

    @Override
    public String toString() {
        return "Task{" +
                "type='" + type + '\'' +
                ", id=" + id +
                ", tags=" + tags +
                '}';
    }

    public static enum State {
        PENDING,
        RUNNING,
        ERROR, DONE
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Task task = (Task) o;

        if (id != null ? !id.equals(task.id) : task.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
